# database-project

Create a database architecture, a script for creating a database in python and various queries.

Here is ER-diagram for database.

![ER-diagram](https://github.com/ssliadniev/database-project/blob/develop/images/ER-diagram_DB.png)

---

### Prerequisites

![](https://img.shields.io/badge/psycopg2--binary-v.2.9.1-brightgreen) ![](https://img.shields.io/badge/pandas-v.1.3.1-brightgreen)

---
    
### Build and run 

Run Python application.

    # From your project directory
    python3 create_db.py \
      --db_name=test_db \
      --db_user=postgres \
      --db_password=your_password \
      --db_host=localhost \
      --db_port=5432 \
      --insert_data=False
---

### Installing

Just git clone this repo and you are good to go.
    
    # sudo apt-get install subversion
    svn export https://github.com/ssliadniev/database-project/trunk/

