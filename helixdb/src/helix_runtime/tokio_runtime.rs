use super::AsyncRuntime;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Tokio based implementation of [`AsyncRuntime`].
#[derive(Clone, Default)]
pub struct TokioRuntime;

impl AsyncRuntime for TokioRuntime {
    type JoinHandle<T> = tokio::task::JoinHandle<T>;

    fn spawn<F, T>(&self, fut: F) -> Self::JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        tokio::spawn(fut)
    }

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(dur))
    }
}
