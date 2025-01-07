# Eventus

Sequential, disk-backed event store database built in Rust.

Still very much WIP. For more info, there's a [blog post explaining how Eventus works behind the scenes](https://theari.dev/blog/building-a-rust-powered-event-store/).

## Credits

This project is a heavily modified fork of [commitlog](https://github.com/zowens/commitlog), which helped provide a very useful backbone to the storing of events. The segment based architecture was inspired by [AxonServer](https://www.axoniq.io/products/axon-server).