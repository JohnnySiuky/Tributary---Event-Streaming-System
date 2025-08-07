# Tributary - Event Streaming System

![Java](https://img.shields.io/badge/Java-17%2B-blue)
![Gradle](https://img.shields.io/badge/Gradle-8.5-green)

## Overview

Tributary is a lightweight event streaming platform built in Java that enables asynchronous communication between distributed system components. It implements core messaging patterns with support for topics, partitions, producers, and consumer groups.

## ✨ Features

- **Topic-based** message organization
- **Partitioned** message queues  
- **Random and manual** partition allocation
- **Consumer groups** with rebalancing
- **Message replay** functionality
- **Thread-safe** implementation
- **Generic** payload support

## 🚀 Quick Start

### Prerequisites
- Java 17+
- Gradle 8.5

### Build & Run
```bash
# Build the project
gradle build
```
# Run the CLI
```bash
gradle run
```

## 💻 CLI Usage Examples
Create Resources
```bash
create topic orders Integer
create partition orders p1
create producer order_producer orders Integer Manual
create consumer group order_group orders Range
create consumer order_group consumer1
```
Produce & Consume Messages
```bash
# Produce message
produce event order_producer orders 12345 p1

# Consume message
consume event consumer1 orders p1

# Replay messages
playback consumer1 orders p1 0
```
View Status
```bash
show topic orders
show consumer group order_group
```

📚 Documentation
- demostrate.mp4
- design.pdf
- final_uml_diagram.jpg
- initialUMLdiagram.jpg

## 🧪 Testing
```bash
# Run tests
gradle test

# Generate coverage report
gradle jacocoTestReport
```
