from queue import Queue
from threading import Thread
from multiprocessing import Pool
from typing import Callable, Iterable


class Worker(Thread):
    def __init__(self, queue, fun):
        Thread.__init__(self)
        self._fun = fun
        self._queue = queue
        self._results = []

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            param = self._queue.get()
            try:
                res = self._fun(param)
                self._results.append(res)
            except BaseException:
                self._empty_queue()
                raise BaseException("Error inside of apply_multithread")
            finally:
                if self._queue.unfinished_tasks > 0:
                    # this can happen if the another thread failed
                    self._queue.task_done()

    def get_result(self):
        return self._results

    def _empty_queue(self):
        while not self._queue.empty():
            self._queue.get()
            self._queue.task_done()


def apply_multithread(
    fun: Callable, iterable: Iterable, n_threads: int = 1
) -> Iterable:
    """
    Wrapper to apply a function on an iterable in multithread

    :param fun: function to be applied (type: function)
    :param iterable: iterable object to apply fuction on (type: iterable)
    :param n_threads: number of threads (type: int)

    :return: result of function (type: list)
    """

    if n_threads == 1:
        return [fun(x) for x in iterable]
    else:
        # Queue of params. Shared across workers
        queue = Queue()
        for param in iterable:
            queue.put(param)
        # Create pool of workers
        workers = []
        for _ in range(n_threads):
            worker = Worker(queue, fun)
            worker.daemon = True
            worker.start()
            workers.append(worker)

        # This prevents MainThread from continuing until all
        # elements in queue are marked as done
        queue.join()
        # Retrieve result from each worker
        results = []
        for worker in workers:
            results = results + worker.get_result()
        return results


def apply_multiprocessing(
    fun: Callable, iterable: Iterable, n_cores: int = 1
) -> Iterable:
    """
    Wrapper to apply a function on an iterable in multiprocessing

    :param fun: function to be applied
    :param iterable: iterable object to apply fuction on
    :param n_cores: number of cores

    :return: result of function
    """

    if n_cores == 1:
        return [fun(x) for x in iterable]
    else:
        pool = Pool(processes=n_cores)
        result_par = pool.map(fun, iterable)
        pool.close()
        pool.join()
        return result_par
