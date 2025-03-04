    fn get_neighbors(
        &self,
        txn: &RoTxn,
        id: &str,
        level: usize,
    ) -> Result<Vec<String>, VectorError> {
        let out_key = Self::out_edges_key(id, "", level);

        let iter = self
            .out_edges_db
            .lazily_decode_data()
            .prefix_iter(&txn, &out_key)?;

        let mut neighbors = Vec::with_capacity(512);
        let prefix_len = OUT_EDGES_PREFIX.len() + id.len() + 1 + level.to_string().len() + 1;

        for result in iter {
            let (key, _) = result?;
            let neighbor_id = String::from_utf8(key[prefix_len..].to_vec())?;
            neighbors.push(neighbor_id);
        }

        Ok(neighbors)
    }

    fn set_neighbors(
        &self,
        txn: &mut RwTxn,
        id: &str,
        level: usize,
        neighbors: &[String],
    ) -> Result<(), VectorError> {
        neighbors
            .iter()
            .try_for_each(|neighbor_id| -> Result<(), VectorError> {
                let out_key = Self::out_edges_key(id, neighbor_id, level);
                let in_key = Self::in_edges_key(neighbor_id, id, level);

                self.out_edges_db.put(txn, &out_key, &())?;
                self.in_edges_db.put(txn, &in_key, &())?;
                Ok(())
            })?;
        Ok(())
    }

    #[inline]
    fn get_vector(&self, txn: &RoTxn, id: &str, level: usize) -> Result<HVector, VectorError> {
        let key = Self::vector_key(id, level);
        match self.vectors_db.get(txn, &key)? {
            Some(bytes) => deserialize(&bytes).map_err(VectorError::from),
            None => Err(VectorError::VectorNotFound),
        }
    }

    #[inline]
    fn put_vector(&self, txn: &mut RwTxn, id: &str, vector: &HVector) -> Result<(), VectorError> {
        let key = Self::vector_key(id, vector.level);
        let serialized = serialize(vector).map_err(VectorError::from)?;
        self.vectors_db.put(txn, &key, &serialized)?;
        Ok(())
    }

    fn search_layer(
        &self,
        txn: &RoTxn,
        query: &HVector,
        entry_point: &HVector,
        ef: usize,
        level: usize,
    ) -> Result<BinaryHeap<DistancedId>, VectorError> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        let distance = entry_point.distance_to(query);

        candidates.push(DistancedId {
            id: entry_point.get_id().to_string(),
            distance,
        });

        results.push(DistancedId {
            id: entry_point.get_id().to_string(),
            distance,
        });

        visited.insert(entry_point.get_id().to_string());

        let expanded_ef = ef.max(10);

        while !candidates.is_empty() {
            let current = candidates.pop().unwrap();

            if results.len() >= expanded_ef {
                if let Some(furthest) = results.peek() {
                    if current.distance > furthest.distance {
                        continue;
                    }
                }
            }

            let neighbors = self.get_neighbors(txn, &current.id, level)?;

            for neighbor_id in neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }

                visited.insert(neighbor_id.clone());
                let neighbor_vector = match self.get_vector(txn, &neighbor_id, level) {
                    Ok(v) => v,
                    Err(_) if level > 0 => match self.get_vector(txn, &neighbor_id, 0) {
                        Ok(v) => v,
                        Err(_) => continue,
                    },
                    Err(_) => continue,
                };

                let distance = neighbor_vector.distance_to(query);

                candidates.push(DistancedId {
                    id: neighbor_id.clone(),
                    distance,
                });

                if results.len() < expanded_ef || distance < results.peek().unwrap().distance {
                    results.push(DistancedId {
                        id: neighbor_id,
                        distance,
                    });

                    if results.len() > expanded_ef {
                        results.pop();
                    }
                }
            }
        }

        Ok(results)
    }

    fn select_neighbors(
        &self,
        txn: &RoTxn,
        candidates: &BinaryHeap<DistancedId>,
        m: usize,
        level: usize,
    ) -> Result<Vec<String>, VectorError> {
        if candidates.len() <= m {
            return Ok(candidates.iter().map(|c| c.id.clone()).collect());
        }

        let mut selected = Vec::with_capacity(m);
        let mut remaining: Vec<_> = candidates.iter().collect();

        remaining.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });

        if !remaining.is_empty() {
            let next = remaining.remove(0);
            selected.push(next.id.clone());
        }

        while selected.len() < m && !remaining.is_empty() {
            let next = remaining.remove(0);
            selected.push(next.id.clone());

            remaining = remaining
                .into_iter()
                .filter(|candidate| {
                    for selected_id in &selected {
                        if &candidate.id == selected_id {
                            continue;
                        }

                        let selected_vector = match self.get_vector(txn, selected_id, level) {
                            Ok(v) => v,
                            Err(_) => return true,
                        };

                        let candidate_vector = match self.get_vector(txn, &candidate.id, level) {
                            Ok(v) => v,
                            Err(_) => return true,
                        };

                        let distance = selected_vector.distance_to(&candidate_vector);

                        if distance < candidate.distance {
                            return false;
                        }
                    }
                    true
                })
                .collect();
        }

        Ok(selected)
    }

    fn insert(&self, txn: &mut RwTxn, data: &[f64]) -> Result<String, VectorError> {
        let random_level = self.get_random_level();

        //let reduced_vec = self.reduce_dims(data);
        //let vector = HVector::from_slice(id.to_string(), 0, reduced_vec.clone());

        let id = uuid::Uuid::new_v4().to_string();
        let vector = HVector::from_slice(id.clone(), random_level, data.to_vec());
        let id = id.as_str();
        self.put_vector(txn, id, &vector)?;

        if random_level > 0 {
            let level0_vector = HVector::from_slice(id.to_string(), 0, data.to_vec());
            self.put_vector(txn, id, &level0_vector)?;
        }

        let mut entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                let entry_point = HVector::from_slice(id.to_string(), random_level, data.to_vec());
                self.set_entry_point(txn, &entry_point)?;
                entry_point
            }
        };

        let curr_id = entry_point.get_id().to_string();
        let mut curr_level = entry_point.get_level();

        if random_level > curr_level {
            entry_point = HVector::from_slice(id.to_string(), random_level, data.to_vec());
            self.set_entry_point(txn, &entry_point)?;
            curr_level = random_level;
        }

        let mut ep_id = curr_id;

        if ep_id != id {
            let neighbors = vec![ep_id.clone()];
            self.set_neighbors(txn, id, 0, &neighbors)?;

            let mut ep_neighbors = self.get_neighbors(txn, &ep_id, 0)?;
            ep_neighbors.push(id.to_string());
            self.set_neighbors(txn, &ep_id, 0, &ep_neighbors)?;
        }

        for level in (1..=random_level).rev() {
            if level <= curr_level {
                let ef = self.config.ef_construction * 2;
                let nearest = self.search_layer(txn, &vector, &entry_point, ef, level)?;

                if nearest.is_empty() {
                    continue;
                }

                let m = if level == 0 {
                    self.config.m
                } else {
                    self.config.m_max
                };

                let neighbors = self.select_neighbors(txn, &nearest, m, level)?;

                self.set_neighbors(txn, id, level, &neighbors)?;

                for neighbor_id in &neighbors {
                    let mut neighbor_neighbors = self.get_neighbors(txn, neighbor_id, level)?;
                    neighbor_neighbors.push(id.to_string());

                    if neighbor_neighbors.len() > m {
                        let neighbor_vector = match self.get_vector(txn, neighbor_id, level) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        let candidates: BinaryHeap<_> = neighbor_neighbors
                            .iter()
                            .filter_map(|n_id| match self.get_vector(txn, n_id, level) {
                                Ok(n_vector) => {
                                    let dist = neighbor_vector.distance_to(&n_vector);
                                    Some(DistancedId {
                                        id: n_id.clone(),
                                        distance: dist,
                                    })
                                }
                                Err(_) => None,
                            })
                            .collect();

                        let pruned = self.select_neighbors(txn, &candidates, m, level)?;
                        self.set_neighbors(txn, neighbor_id, level, &pruned)?;
                    } else {
                        self.set_neighbors(txn, neighbor_id, level, &neighbor_neighbors)?;
                    }
                }

                if !nearest.is_empty() {
                    ep_id = nearest.peek().unwrap().id.clone();
                }
            }
        }

        Ok(id.to_string())
    }

    fn get_all_vectors(&self, txn: &RoTxn) -> Result<Vec<HVector>, VectorError> {
        let mut vectors = Vec::new();

        let prefix_iter = self.vectors_db.prefix_iter(txn, VECTOR_PREFIX)?;
        for result in prefix_iter {
            let (_, value) = result?;
            let vector: HVector = deserialize(&value)?;
            vectors.push(vector);
        }
        Ok(vectors)
    }
}
