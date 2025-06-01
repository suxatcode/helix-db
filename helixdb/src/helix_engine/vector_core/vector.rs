use crate::{
    helix_engine::types::{GraphError, VectorError},
    protocol::{
        filterable::{Filterable, FilterableType},
        return_values::ReturnValue,
        value::Value,
    },
};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use std::{cmp::Ordering, collections::HashMap};

pub mod encoding {
    use std::{fmt::Debug, ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign}};

    pub trait Encoding:
        Default
        + Copy
        + Mul<Output = Self>
        + Add<Output = Self>
        + Sub<Output = Self>
        + Div<Output = Self>
        + AddAssign<Self>
        + SubAssign<Self>
        + MulAssign<Self>
        + DivAssign<Self>
        + Into<f64>
        + Debug
    {
        fn from_be_bytes(bytes: &[u8]) -> Self;

        #[inline(always)]
        fn to_f64(self) -> f64 {
            self.into()
        }

        #[inline(always)]
        fn add_f64(self, other: Self) -> f64 {
            self.to_f64() + other.to_f64()
        }

        #[inline(always)]
        fn mul_f64(self, other: Self) -> f64 {
            self.to_f64() * other.to_f64()
        }

        #[inline(always)]
        fn sub_f64(self, other: Self) -> f64 {
            self.to_f64() - other.to_f64()
        }

        #[inline(always)]
        fn div_f64(self, other: Self) -> f64 {
            self.to_f64() / other.to_f64()
        }
    }

    impl Encoding for f32 {
        #[inline(always)]
        fn from_be_bytes(bytes: &[u8]) -> Self {
            f32::from_be_bytes(bytes.try_into().unwrap())
        }
    }
    impl Encoding for f64 {
        #[inline(always)]
        fn from_be_bytes(bytes: &[u8]) -> Self {
            f64::from_be_bytes(bytes.try_into().unwrap())
        }
    }
}

#[repr(C, align(16))]
#[derive(Clone, Debug)]
// #[serde(bound(deserialize = "E: Deserialize<'de> + Default + Copy"))]
pub struct HVector<E, const DIMENSION: usize>
where
    E: encoding::Encoding,
{
    pub id: u128,
    pub is_deleted: bool,
    pub level: u8,
    pub distance: Option<f64>,
    pub data: [E; DIMENSION],
    pub properties: Option<HashMap<String, Value>>,
}

impl<E, const DIMENSION: usize> Eq for HVector<E, DIMENSION> where E: encoding::Encoding {}

impl<E, const DIMENSION: usize> PartialEq for HVector<E, DIMENSION>
where
    E: encoding::Encoding,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<E, const DIMENSION: usize> PartialOrd for HVector<E, DIMENSION>
where
    E: encoding::Encoding,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.distance.partial_cmp(&self.distance)
    }
}

impl<E, const DIMENSION: usize> Ord for HVector<E, DIMENSION>
where
    E: encoding::Encoding,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

pub trait DistanceCalc<E, const DIMENSION: usize>
where
    E: encoding::Encoding,
{
    fn distance(
        from: &HVector<E, DIMENSION>,
        to: &HVector<E, DIMENSION>,
    ) -> Result<f64, VectorError>;
}

impl<E, const DIMENSION: usize> DistanceCalc<E, DIMENSION> for HVector<E, DIMENSION>
where
    E: encoding::Encoding,
{
    #[inline(always)]
    #[cfg(feature = "cosine")]
    fn distance(
        from: &HVector<E, DIMENSION>,
        to: &HVector<E, DIMENSION>,
    ) -> Result<f64, VectorError> {
        from.cosine_similarity(to)
    }
}

impl<E, const DIMENSION: usize> HVector<E, DIMENSION>
where
    E: encoding::Encoding,
{
    // const _ASSERT_DIM_LE_U16_MAX: [(); (u16::MAX as usize) - DIMENSION + 1] =
    //     [(); (u16::MAX as usize) - DIMENSION + 1];

    #[inline(always)]
    pub fn new(data: [E; DIMENSION]) -> Self {
        let id = uuid::Uuid::new_v4().as_u128();
        HVector {
            id,
            is_deleted: false,
            level: 0,
            data,
            distance: None,
            properties: None,
        }
    }

    #[inline(always)]
    pub fn from_slice(level: u8, data: &[E; DIMENSION]) -> Self {
        let id = uuid::Uuid::new_v4().as_u128();
        HVector {
            id,
            is_deleted: false,
            level,
            data: *data,
            distance: None,
            properties: None,
        }
    }

    #[inline(always)]
    pub fn get_data(&self) -> &[E; DIMENSION] {
        &self.data
    }

    #[inline(always)]
    pub fn get_id(&self) -> u128 {
        self.id
    }

    #[inline(always)]
    pub fn get_level(&self) -> u8 {
        self.level
    }

    /// Converts the HVector to an vec of bytes by accessing the data field directly
    /// and converting each f64 to a byte slice
    pub fn to_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                DIMENSION * std::mem::size_of::<E>(),
            )
        }
    }

    /// Converts a byte array into a HVector by chunking the bytes into f64 values
    pub fn from_bytes(id: u128, level: u8, bytes: &[u8]) -> Result<Self, VectorError> {
        if bytes.len() % std::mem::size_of::<E>() != 0 {
            return Err(VectorError::InvalidVectorData);
        }

        let mut data = [E::default(); DIMENSION];
        let chunks = bytes.chunks_exact(std::mem::size_of::<E>());

        for (i, chunk) in chunks.enumerate() {
            let value = E::from_be_bytes(chunk.try_into().unwrap());
            data[i] = value;
        }

        let v = HVector {
            id,
            is_deleted: false,
            level,
            data,
            distance: None,
            properties: None,
        };

        Ok(v)
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline(always)]
    pub fn distance_to(&self, other: &HVector<E, DIMENSION>) -> Result<f64, VectorError> {
        HVector::distance(self, other)
    }

    #[inline(always)]
    pub fn set_distance(&mut self, distance: f64) {
        self.distance = Some(distance);
    }

    #[inline(always)]
    pub fn get_distance(&self) -> f64 {
        match self.distance {
            Some(distance) => distance,
            None => panic!("Distance is not set for vector: {}", self.get_id()),
        }
    }

    #[inline(always)]
    #[cfg(feature = "cosine")]
    fn cosine_similarity(&self, other: &HVector<E, DIMENSION>) -> Result<f64, VectorError> {
        let len = self.data.len();
        let other_len = other.data.len();

        if len != other_len {
            println!("mis-match in vector dimensions!\n{} != {}", len, other_len);
            return Err(VectorError::InvalidVectorLength);
        }
        //debug_assert_eq!(len, other.data.len(), "Vectors must have the same length");

        #[cfg(target_feature = "avx2")]
        {
            return self.cosine_similarity_avx2(other);
        }

        let mut dot_product: f64 = 0.0;
        let mut magnitude_a: f64 = 0.0;
        let mut magnitude_b: f64 = 0.0;

        const CHUNK_SIZE: usize = 8;
        let chunks = len / CHUNK_SIZE;
        let remainder = len % CHUNK_SIZE;

        for i in 0..chunks {
            let offset = i * CHUNK_SIZE;
            let a_chunk = &self.data[offset..offset + CHUNK_SIZE];
            let b_chunk = &other.data[offset..offset + CHUNK_SIZE];

            let mut local_dot = 0.0;
            let mut local_mag_a = 0.0;
            let mut local_mag_b = 0.0;

            for j in 0..CHUNK_SIZE {
                let a_val = a_chunk[j];
                let b_val = b_chunk[j];
                local_dot += a_val.mul_f64(b_val);
                local_mag_a += a_val.mul_f64(a_val);
                local_mag_b += b_val.mul_f64(b_val);
            }

            dot_product += local_dot;
            magnitude_a += local_mag_a;
            magnitude_b += local_mag_b;
        }

        let remainder_offset = chunks * CHUNK_SIZE;
        for i in 0..remainder {
            let a_val = self.data[remainder_offset + i];
            let b_val = other.data[remainder_offset + i];
            dot_product += a_val.mul_f64(b_val);
            magnitude_a += a_val.mul_f64(a_val);
            magnitude_b += b_val.mul_f64(b_val);
        }

        Ok(dot_product / (magnitude_a.sqrt() * magnitude_b.sqrt()))
    }

    // SIMD implementation using AVX2 (256-bit vectors)
    #[cfg(target_feature = "avx2")]
    #[inline(always)]
    fn cosine_similarity_avx2(a: &[f64], b: &[f64]) -> f64 {
        use std::arch::x86_64::*;

        let len = a.len();
        let chunks = len / 4; // AVX2 processes 4 f64 values at once

        unsafe {
            let mut dot_product = _mm256_setzero_pd();
            let mut magnitude_a = _mm256_setzero_pd();
            let mut magnitude_b = _mm256_setzero_pd();

            for i in 0..chunks {
                let offset = i * 4;

                // Load data - handle unaligned data
                let a_chunk = _mm256_loadu_pd(&a[offset]);
                let b_chunk = _mm256_loadu_pd(&b[offset]);

                // Calculate dot product and magnitudes in parallel
                dot_product = _mm256_add_pd(dot_product, _mm256_mul_pd(a_chunk, b_chunk));
                magnitude_a = _mm256_add_pd(magnitude_a, _mm256_mul_pd(a_chunk, a_chunk));
                magnitude_b = _mm256_add_pd(magnitude_b, _mm256_mul_pd(b_chunk, b_chunk));
            }

            // Horizontal sum of 4 doubles in each vector
            let dot_sum = horizontal_sum_pd(dot_product);
            let mag_a_sum = horizontal_sum_pd(magnitude_a);
            let mag_b_sum = horizontal_sum_pd(magnitude_b);

            // Handle remainder elements
            let mut dot_remainder = 0.0;
            let mut mag_a_remainder = 0.0;
            let mut mag_b_remainder = 0.0;

            let remainder_offset = chunks * 4;
            for i in remainder_offset..len {
                let a_val = a[i];
                let b_val = b[i];
                dot_remainder += a_val * b_val;
                mag_a_remainder += a_val * a_val;
                mag_b_remainder += b_val * b_val;
            }

            // Combine SIMD and scalar results
            let dot_product_total = dot_sum + dot_remainder;
            let magnitude_a_total = (mag_a_sum + mag_a_remainder).sqrt();
            let magnitude_b_total = (mag_b_sum + mag_b_remainder).sqrt();

            dot_product_total / (magnitude_a_total * magnitude_b_total)
        }
    }

    // Helper function to sum the 4 doubles in an AVX2 vector
    #[cfg(target_feature = "avx2")]
    #[inline(always)]
    unsafe fn horizontal_sum_pd(__v: __m256d) -> f64 {
        use std::arch::x86_64::*;

        // Extract the high 128 bits and add to the low 128 bits
        let sum_hi_lo = _mm_add_pd(_mm256_castpd256_pd128(__v), _mm256_extractf128_pd(__v, 1));

        // Add the high 64 bits to the low 64 bits
        let sum = _mm_add_sd(sum_hi_lo, _mm_unpackhi_pd(sum_hi_lo, sum_hi_lo));

        // Extract the low 64 bits as a scalar
        _mm_cvtsd_f64(sum)
    }
}

impl<E, const DIMENSION: usize> Filterable for HVector<E, DIMENSION>
where
    E: encoding::Encoding,
{
    fn type_name(&self) -> FilterableType {
        FilterableType::Vector
    }

    fn id(&self) -> &u128 {
        &self.id
    }

    fn uuid(&self) -> String {
        uuid::Uuid::from_u128(self.id).to_string()
    }

    fn label(&self) -> &str {
        "vector"
    }

    fn from_node(&self) -> u128 {
        unreachable!()
    }

    fn from_node_uuid(&self) -> String {
        unreachable!()
    }

    fn to_node(&self) -> u128 {
        unreachable!()
    }

    fn to_node_uuid(&self) -> String {
        unreachable!()
    }

    fn properties(self) -> Option<HashMap<String, Value>> {
        let mut result = HashMap::new();
        result.insert(
            "data".to_string(),
            Value::Array(self.data.iter().map(|f| Value::F64(f.to_f64())).collect()), // look into this
        );
        if let Some(properties) = self.properties.clone() {
            for (key, value) in properties.into_iter() {
                result.insert(key, value);
            }
        }
        Some(result)
    }

    fn properties_mut(&mut self) -> &mut Option<HashMap<String, Value>> {
        &mut self.properties
    }

    fn properties_ref(&self) -> &Option<HashMap<String, Value>> {
        &self.properties
    }

    // TODO: Implement this
    fn check_property(&self, key: &str) -> Result<&Value, GraphError> {
        match &self.properties {
            Some(properties) => properties
                .get(key)
                .ok_or(GraphError::ConversionError(format!(
                    "Property {} not found",
                    key
                ))),
            None => Err(GraphError::ConversionError(format!(
                "Property {} not found",
                key
            ))),
        }
    }

    fn find_property(
        &self,
        _key: &str,
        _secondary_properties: &HashMap<String, ReturnValue>,
        _property: &mut ReturnValue,
    ) -> Option<&ReturnValue> {
        unreachable!()
    }
}
