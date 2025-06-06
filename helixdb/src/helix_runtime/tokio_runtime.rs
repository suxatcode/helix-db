use super::AsyncRuntime;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Tokio based implementation of [`AsyncRuntime`].
#[derive(Clone, Default)]
pub struct TokioRuntime;

impl AsyncRuntime for TokioRuntime {
    type JoinHandle<T> = TokioJoinHandle<T>
    where
        T: Send + 'static;

    fn spawn<F, T>(&self, fut: F) -> Self::JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        TokioJoinHandle(tokio::spawn(fut))
    }

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(dur))
    }
}

/// Wrapper around Tokio's [`JoinHandle`] that unwraps the result.
pub struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for TokioJoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        match inner.poll(cx) {
            Poll::Ready(Ok(val)) => Poll::Ready(val),
            Poll::Ready(Err(err)) => panic!("Join error: {}", err),
            Poll::Pending => Poll::Pending,
        }
    }
}
