# Raft-rs
An understandable, fast, scalable and optimized Raft implementation. 

## Note
- This project is still under development and is not yet production-ready. It is not recommended to use this in production environments.

- We are actively working on this project to make it better and more reliable. If you have any suggestions or feedback, please feel free to open an issue or a pull request.

- Expect breaking changes in the API and the implementation.

- Release version 0.1.0 is planned to be released by **August 16th, 2024**.

## Goals
- [x] Understandable
- [x] Fast
- [x] Scalable
- [x] Zero-Copy support
- [x] Default Leader 
- [x] Leadership preference
- [x] Log compaction

## To-Do
- [ ] Production-ready
- [ ] io_uring support
- [ ] CI integration improvements
- [ ] Complete batch write implementation
- [ ] Improve Log compaction
- [ ] Improve error handling
- [ ] RDMA support
- [ ] Add more comprehensive tests
- [ ] Enhance documentation
- [ ] Implement dynamic cluster membership changes
- [ ] Deterministic Simulation Testing
- [ ] Benchmarking
- [ ] Update to the latest version of Rust and Dependencies 
- [ ] Cargo clippy and cargo fmt integration


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
For any questions or feedback, please reach out to [vaibhaw.vipul@example.com].
