# Architecture

We use two different architectures depending on the workload -- **pull** and **push**.

## Pull Mode

In this version of our system, workers poll brokers for tasks to execute. Each broker exposes `GetTask` and `SetTaskStatus` RPC endpoints that workers can use to fetch pending tasks and update them once complete, respectively. Historically, this model has been paired with SQLite, and has been processing nearly all of Sentry's asynchronous tasks for a while now.

## Push Mode

In push mode, data flows in the opposite direction -- workers now have a `PushTask` endpoint that brokers use to "push" tasks for execution. Brokers still expose a `SetTaskStatus` endpoint for workers to report task completion or failure. This model is meant to be paired with AlloyDB, but in its current state, it _can_ technically be used with SQLite.

<ENTER MERMAID DIAGRAM HERE>

In push mode, we spin up two thread pools to separately fetch pending activations from the store and then send them off to the workers. The pool that fetches tasks is called the `FetchPool` while the pool that pushes tasks is called the `PushPool`.

*Why are these components separate?* Because it allows us to process more work concurrently and thereby increase throughput by a significant amount. Fetching and pushing tasks are both heavy on IO, which means they are mostly waiting. If we fetch and push tasks sequentially, we are wasting resources because we _could_ be fetching while we are pushing (instead of waiting) and we _could_ be pushing while we are fetching (instead of waiting).

These two pools are connected by a MPMC (multiple producer, multiple consumer) channel which we call the "push queue." Once a fetch thread receives an activation, it places it on one end of the push queue. On the other end of the push queue sits a push thread waiting for another activation to send.
