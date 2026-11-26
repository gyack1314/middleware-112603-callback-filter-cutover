"""常见入口名别名。"""

from callback_proxy import (  # noqa: F401
    CallbackProxy,
    CallbackAgent,
    create_proxy,
    main,
    run_scenario,
    simulate,
)

run = run_scenario
Proxy = CallbackProxy

if __name__ == "__main__":
    raise SystemExit(main())
