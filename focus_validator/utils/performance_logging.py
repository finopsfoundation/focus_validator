import functools
import logging
import os
import time
from typing import Any, Callable, Dict, Optional

try:
    import psutil  # type: ignore[import-untyped]

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

perfLogger = logging.getLogger("performance")


class PerformanceTracker:
    def __init__(self):
        self.startTime = None
        self.operation = None
        self.context = {}

    def start(self, operation: str, context: Optional[Dict[str, Any]] = None):
        self.operation = operation
        self.context = context or {}
        self.startTime = time.time()

        # Log operation start with context
        contextStr = (
            ", ".join(f"{k}={v}" for k, v in self.context.items())
            if self.context
            else "no context"
        )
        perfLogger.info(f"Started {operation} ({contextStr})")

    def finish(self, additionalContext: Optional[Dict[str, Any]] = None):
        if self.startTime is None:
            return

        duration = time.time() - self.startTime

        # Get memory usage if psutil is available
        memoryMb = 0
        if HAS_PSUTIL:
            try:
                process = psutil.Process(os.getpid())
                memoryMb = process.memory_info().rss / 1024 / 1024
            except Exception:
                pass

        # Combine context
        fullContext = {**self.context, **(additionalContext or {})}
        contextStr = (
            ", ".join(f"{k}={v}" for k, v in fullContext.items()) if fullContext else ""
        )

        if HAS_PSUTIL and memoryMb > 0:
            perfLogger.info(
                f"Completed {self.operation} in {duration:.3f}s (memory: {memoryMb:.1f}MB) — {contextStr}"
            )
        else:
            perfLogger.info(
                f"Completed {self.operation} in {duration:.3f}s — {contextStr}"
            )

        # Reset
        self.startTime = None
        self.operation = None
        self.context = {}


def logPerformance(
    operation: str, includeArgs: bool = False, context: Optional[dict[str, Any]] = None
) -> Callable:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            operationName = operation or f"{func.__module__}.{func.__qualname__}"

            # Build context from args if requested
            context = {}
            if includeArgs and args:
                if hasattr(args[0], "__class__"):
                    context["instance"] = args[0].__class__.__name__
                if len(args) > 1:
                    context["args_count"] = len(args) - 1
            if includeArgs and kwargs:
                context.update({k: str(v)[:50] for k, v in kwargs.items()})

            tracker = PerformanceTracker()
            tracker.start(operationName, context)

            try:
                result = func(*args, **kwargs)

                # Add result context if available
                resultContext = {}
                if hasattr(result, "__len__"):
                    try:
                        resultContext["result_size"] = len(result)
                    except Exception:
                        pass

                tracker.finish(resultContext)
                return result
            except Exception as e:
                tracker.finish({"error": str(e)[:100]})
                raise

        return wrapper

    return decorator


def logTiming(logger: logging.Logger, operation: str):
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            startTime = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - startTime
                logger.debug(f"{operation} completed in {duration:.3f}s")
                return result
            except Exception as e:
                duration = time.time() - startTime
                logger.error(f"{operation} failed after {duration:.3f}s: {e}")
                raise

        return wrapper

    return decorator
