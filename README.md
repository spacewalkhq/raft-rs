# Raft-rs
An understandable, fast, scalable and optimized implementation of [Raft algoritm](https://en.wikipedia.org/wiki/Raft_(algorithm)).
It is asynchronous(built on tokio runtime) and supports zero-copy. It does not assume logs to be non-malicious, if corrupted, it will repair the logs via peer-to-peer communication. 

## Note
- This project is still under development and is not yet production-ready. It is not recommended to use this in production environments.

- We are actively working on this project to make it better and more reliable. If you have any suggestions or feedback, please feel free to open an issue or a pull request. This is true until we reach version 1.0.0.

- Release every 2 weeks.

## Goals
- [x] Understandable
- [x] Fast
- [x] Scalable
- [x] Zero-Copy support
- [x] Asynchronous
- [x] Default Leader 
- [x] Leadership preference
- [x] Log compaction
- [x] Tigerbeetle style replica repair
- [x] Dynamic cluster membership changes support

## To-Do
- [ ] Production-ready
- [ ] Test replica repair thoroughly
- [ ] Test for dynamic cluster membership changes
- [ ] io_uring support
- [ ] Complete batch write implementation
- [ ] Improve Log compaction
- [ ] Improve error handling
- [ ] RDMA support
- [ ] Add more comprehensive tests
- [ ] Enhance documentation
- [ ] Deterministic Simulation Testing
- [ ] Benchmarking

## How to Run the Project
1. Ensure you have Rust installed. If not, follow the instructions [here](https://www.rust-lang.org/tools/install).
2. Clone the repository:
   ```sh
   git clone https://github.com/your-username/raft-rs.git
   cd raft-rs
   ```
3. Run the project:
   ```sh
   cargo run --example simple_run
   ```
4. Release the project:
   ```sh
   cargo build --release
   ```

## Contributing
Contributions are welcome! If you have any ideas, suggestions, or issues, please feel free to open an issue or a pull request. We aim to make this project better with your help.

## License
This project is licensed under the MIT License. For more information, please refer to the [LICENSE](LICENSE) file.

## Contact
For any questions or feedback, please reach out to [vaibhaw.vipul@gmail.com].
