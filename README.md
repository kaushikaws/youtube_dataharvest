# youtube_dataharvest
This project is designed to harvest and warehouse YouTube data using Python programming language, PostgreSQL, MongoDB, and Streamlit. The goal is to collect, store, and analyze YouTube data for various purposes such as content recommendation, trend analysis, and user behavior insights.

**Tools Used** 
**Python Programming Language**
Python is used for its versatility, ease of use, and a rich ecosystem of libraries, making it suitable for web scraping, data processing, and analysis.

**Google API Client:**
The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

**AWS RDS MYSQL:** 
AWS RDS MYSQL is employed as the relational database management system (RDBMS) to store structured data efficiently. It provides ACID compliance and supports complex queries.

**MongoDB:** 
MongoDB is used as the NoSQL database to store semi-structured and unstructured data. Its flexible schema and scalability make it suitable for handling diverse types of data.

**Streamlit:**
Streamlit is utilized for building interactive and customizable web-based data dashboards. It allows for easy visualization of the harvested YouTube data.
