# Open Lakehouses for BI to AI with Apache Hudi

Welcome to the code repository for **Open Lakehouses for BI to AI with Apache Hudi**, published by Packt. This repository contains hands-on examples, tutorials, and code samples that demonstrate Apache Hudi's capabilities for building robust data lakes.

## 📖 About the Book

*Open Lakehouses for BI to AI with Apache Hudi* is a comprehensive guide to [Apache Hudi](https://hudi.apache.org/), covering everything from basic concepts to advanced production patterns. The book provides practical examples and real-world scenarios to help you master Hudi for your data engineering needs.

## 🗂️ Repository Structure

The code is organized by chapter. Each folder is self-contained, with its own README, sample data, and run scripts.

| Folder | Chapter | Runs on |
|---|---|---|
| [`chapter02`](chapter02) | Chapter 2: Apache Hudi Essentials and Quickstart | Spark 3.5, Hudi 1.2.0; Docker optional |
| [`chapter03`](chapter03) | Chapter 3: Apache Hudi ingestion patterns and write workflows | Spark 3.5, Hudi 1.2.0 |
| [`chapter04`](chapter04) | Chapter 4: Querying Hudi tables | Spark 3.5, Hudi 1.2.1; Python for `ai_ml/` |
| [`chapter05`](chapter05) | Chapter 5: Managing Tables Using Table Services | Spark 3.5, Hudi 1.2.1 |
| [`chapter06`](chapter06) | Chapter 6: Building a Real-Time Marketplace Payout Lakehouse with Apache Hudi | Spark 3.5, Hudi 1.2.1, Docker (Kafka) |
| [`chapter07`](chapter07) | Chapter 7: Building a Streaming and CDC Lakehouse with Apache Hudi and Flink | Docker only (Flink 1.20, Hudi 1.2.0) |
| [`chapter09`](chapter09) | Chapter 9: Advanced Lakehouse Capabilities | Spark 3.5 and 4.1, Flink 1.20, Hudi 1.2.1 |
| [`chapter10`](chapter10) | Chapter 10: Designing Production Lakehouse Pipelines | Design files and configuration for review (Kafka, S3, DynamoDB, Glue); Hudi 1.2.1 |

## 📋 Prerequisites

Each chapter's README lists the exact versions it was tested with. In general you need:

### Software Requirements
- **Apache Spark** 3.5 with Scala 2.12 (tested on 3.5.6). Chapter 9's VARIANT examples also need Spark 4.1 with Scala 2.13.
- **Java** 11 or 17
- **Apache Hudi** 1.2.0 or later. The scripts pull the bundles via `--packages` or download them.
- **Apache Flink** 1.20 for Chapter 9's Flink examples (Chapter 7 runs Flink inside Docker)
- **Docker** and Docker Compose for Chapters 6 and 7 (optional for Chapter 2)
- **Python** 3 for Chapter 4's `ai_ml/` examples

### Hardware Requirements
- At least 4 GB RAM for Spark; Chapter 7's Docker stack (Flink, Kafka, MySQL) needs 6-8 GB
- 2+ CPU cores recommended
- ~2 GB disk space for sample data and tables

## 🚀 Getting Started

1. Clone this repository
2. Navigate to the chapter you want to explore
3. Follow the chapter-specific README for detailed setup instructions
4. Each chapter is self-contained with its own dataset and examples

## 🤝 Contributing

Found an issue or want to improve the examples? Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with clear commit messages
4. Submit a pull request

## 📄 License

This code is provided as supplementary material for *Open Lakehouses for BI to AI with Apache Hudi* and is released under the [MIT License](LICENSE).

## 📧 Support

For questions about the book or code examples:
- Check the [Issues](https://github.com/PacktPublishing/Open-Lakehouses-for-BI-to-AI-with-Apache-Hudi/issues) page
- Refer to the [Apache Hudi documentation](https://hudi.apache.org/)
- Visit the [Apache Hudi community](https://hudi.apache.org/community/)

---

Happy learning with Apache Hudi! 🎉
