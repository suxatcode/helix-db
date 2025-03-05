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

