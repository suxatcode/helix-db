use protocol::value::Value;

pub mod macros {
    #[macro_export]
    /// Creates array of pairs which each represent the property key and corresponding value.
    ///
    /// ## Example Use
    /// ```rust
    /// use helix_engine::props;
    /// use protocol::value::Value;
    ///
    /// let properties: Vec<(String, Value)> = props! {
    ///     "name" => "Will",
    ///     "age" => 21,
    /// };
    ///
    /// assert_eq!(properties.len(), 2);
    /// ```
    macro_rules! props {
        () => {
            vec![]
        };
        ($($key:expr => $value:expr),* $(,)?) => {
            vec![
                $(
                    (String::from($key), $value.into()),
                )*
            ]
        };

    }
}
