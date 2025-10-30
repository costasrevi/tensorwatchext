# 🧠 TensorWatchExt

**TensorWatchExt** is an extended version of Microsoft’s [TensorWatch](https://github.com/microsoft/tensorwatch) — redesigned for **real-time streaming data visualization** inside **JupyterLab** and **Jupyter Notebook** environments.  
It adds support for **Apache Kafka**, **PyKafka**, dynamic UI controls, and multithreaded data handling for **high-throughput stream monitoring**.

---

## 🎓 Academic Context

This project is part of the **Master’s Thesis** of  
**Konstantinos Revythis**  
at the **Technical University of Crete (TUC)**,  
School of Electrical and Computer Engineering.  

The work focuses on extending the TensorWatch framework for **real-time distributed data visualization** and **stream analytics integration** with **Kafka-based architectures**.

---

## 🚀 Features

✅ **Real-time Visualization**  
- Stream and visualize data live from Kafka or PyKafka sources.  
- Interactive plots update continuously without restarting the notebook.

✅ **Kafka Integration**  
- Built-in connectors for both **Confluent Kafka** and **PyKafka**.  
- Supports JSON, Pickle, XML, Avro, and Protobuf message formats.  

✅ **Multithreading and Scalability**  
- Background threads for data ingestion to avoid blocking the UI.  
- Handles thousands of messages per second.

✅ **Interactive Jupyter UI**  
- Widgets to change plot style, size, color, and layout on the fly.  
- Easy reset and reconfiguration without restarting the stream.

✅ **Lazy Logging Mode**  
- Efficient data handling via lazy logging and streaming buffers.  
- Compatible with TensorWatch’s `Watcher` and `Visualizer` APIs.

---

## 🧩 Installation

Install via PyPI:

```bash
pip install tensorwatchext
