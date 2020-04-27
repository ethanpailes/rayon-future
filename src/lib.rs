use std::{
    sync::{Arc, Mutex},
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use rayon;
use crossbeam::channel;

/// Immediately spawn the given computation and return a handle to its result
pub fn spawn<T, F>(f: F) -> RayonFuture<T>
    where F: FnOnce() -> T + Send + 'static,
          T: Send + 'static,
{
    let (send, recv) = channel::bounded(1);

    let fut = RayonFuture{
        state: Arc::new(Mutex::new(State {
            recv,
            waker: None,
        })),
    };
    let fut_st = fut.state.clone();

    rayon::spawn(move || {
        let result = f();
        send.send(result).unwrap();

        // check to see if the future has already been polled and is now
        // waiting to get polled again.
        let mut st = fut_st.lock().expect("rayon future lock");
        if let Some(waker) = st.waker.take() {
            waker.wake();
        };
    });

    fut
}

/// A future representing some compute intensive/blocking IO work that has
/// been offloaded to the rayon thread pool. When the computation is done
/// the future will complete.
pub struct RayonFuture<T> {
    state: Arc<Mutex<State<T>>>,
}

/// internal future state gaurded by a lock
struct State<T> {
    recv: channel::Receiver<T>,
    waker: Option<Waker>,
}

impl<T> Future for RayonFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<T> {
        let mut st = self.state.lock().expect("rayon future lock");
        match st.recv.try_recv() {
            Ok(r) => Poll::Ready(r),
            Err(channel::TryRecvError::Empty) => {
                st.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Err(channel::TryRecvError::Disconnected) => {
                panic!("unexpected rayon future hangup");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;
    use async_std;

    #[test]
    fn it_spawns_a_computation() {
        let h = async_std::task::spawn(spawn(move || {
            sleep(Duration::from_millis(20));
            1
        }));
        let res = async_std::task::block_on(h);
        assert_eq!(res, 1);
    }

    #[test]
    fn it_gets_the_value_when_polling_is_late() {
        let h = async_std::task::spawn(spawn(move || {
            sleep(Duration::from_millis(20));
            1
        }));
        sleep(Duration::from_millis(50));

        let res = async_std::task::block_on(h);
        assert_eq!(res, 1);
    }
}
