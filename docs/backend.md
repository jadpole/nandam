# Backend Architecture

The Backend is architectured around "workspaces", "services", and "processes".

- **Process** is the unit of execution, akin to a function with arguments and a
  result, which can execute **actions** (such as invoke tools) and might act in
  response to **events** (such as tool results).

- **Workspace** is a persistent environment where processes execute.  Since the
  backend is deployed across multiple replica, API requests are translated into
  messages that are sent to the correct Backend instance via Redis.

- **Services** provide features to the backend, such as **tools** that provide
  an interface to spawn new processes.

The Workspace acts as an orchestrator that runs in the backend, but processes
may run elsewhere.  For example, client applications can provide "tools", such
that invoke requests are sent to the client, which executes it, then sends the
result to the Backend.

The Process interface therefore provides a generic API for:

- Process execution (via "on_spawn" to initialize the process);
- Signal handlers, such as "on_sigterm";
- Saving "progress" (arbitrary data sent for debugging) and "result".
- Sending "actions" and receiving "events".

The core interface is defined in "backend/server/context.py", allowing other
modules to implement different services and processes as they wish.


## Remote services

Remote apps can attach "services" to a Workspace by calling:

```
POST /api/workspace/service
    <- { config: ServiceConfig, ... TODO ... }
    -> { ... TODO ... }
```

The `ServiceConfig` is then used by the Workspace to instantiate an `NdService`
subclass and a `RemoteWorkerSecret` is generated, acting as the "authorization"
by the app to poll actions and send events.

The `NdService` and `NdProcess` interfaces provide full generality, and thus,
how they are used depends on the desired behaviour.  For example,

- One service may provide "API tools" that are invoked by calling an endpoint.

- Another may provide "PULL-style tools":
    - The remote app calls an endpoint (using the `RemoteWorkerSecret`) to poll
      for tool call actions.
    - Whenever a tool of the remote service is invoked in the remote service,
      a `RemoteProcessSecret` is generated, acting as the "authorization" for
      the process.  It is returned in the poll result.
    - As the process executes, the worker uses the `RemoteProcessSecret` to send
      actions, progress and the result, but also, to receive "events", such as
      the result of a child process spawned via a tool call.
    - Once a result has been received, the process can no longer send updates.
      Using the secret will result in `BadProcessError`.

## Custom processes

A remote app can also spawn a "custom process" for a session.  When a process is
created, its `NdAuth` is assigned to the Process URI in `WorkspaceContext`, then
used in child processes.
