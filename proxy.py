"""简洁导入别名：``from proxy import CallbackProxy``。"""

from callback_proxy import (  # noqa: F401
    CallbackProxy,
    CallbackAgent,
    SubscriptionProxy,
    CallbackRouter,
    ProxyError,
    NotFound,
    RevisionConflict,
    RerouteNotEligible,
    run_scenario,
)

Proxy = CallbackProxy
Engine = CallbackProxy

__all__ = [
    "CallbackProxy",
    "CallbackAgent",
    "SubscriptionProxy",
    "CallbackRouter",
    "Proxy",
    "Engine",
    "ProxyError",
    "NotFound",
    "RevisionConflict",
    "RerouteNotEligible",
    "run_scenario",
]


if __name__ == "__main__":  # pragma: no cover
    from callback_proxy import main

    raise SystemExit(main())
