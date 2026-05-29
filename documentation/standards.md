## Configuration

As we enhance taskbroker with more features, the number of configuration options expands. As such, it's important to keep them organized and establish naming conventions.

### Batches

If you are adding any kind of batching feature, you may need one or more of the following options.

| Name |
| <feature>_batch_len   | Maximum number of elements a batch can contain.
| <feature>_batch_size  | Maximum number of bytes a batch can contain.
| <feature>_batch_flush
