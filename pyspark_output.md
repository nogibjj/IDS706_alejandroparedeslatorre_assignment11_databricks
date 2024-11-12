## Test Operation

### Output:

```
This is a test output.
```

## Initiating Extract

### Output:

```
Fetching data from API.
```

## Data Extracted

### Output:

```
|    | alpha_two_code   | country       | domains            | name                                          | state-province   | web_pages                       |
|---:|:-----------------|:--------------|:-------------------|:----------------------------------------------|:-----------------|:--------------------------------|
|  0 | US               | United States | ['marywood.edu']   | Marywood University                           |                  | ['http://www.marywood.edu']     |
|  1 | US               | United States | ['lindenwood.edu'] | Lindenwood University                         |                  | ['http://www.lindenwood.edu/']  |
|  2 | US               | United States | ['sullivan.edu']   | Sullivan University                           |                  | ['https://sullivan.edu/']       |
|  3 | US               | United States | ['fscj.edu']       | Florida State College at Jacksonville         |                  | ['https://www.fscj.edu/']       |
|  4 | US               | United States | ['xavier.edu']     | Xavier University                             |                  | ['https://www.xavier.edu/']     |
|  5 | US               | United States | ['tusculum.edu']   | Tusculum College                              |                  | ['https://home.tusculum.edu/']  |
|  6 | US               | United States | ['cst.edu']        | Claremont School of Theology                  |                  | ['https://cst.edu/']            |
|  7 | US               | United States | ['columbiasc.edu'] | Columbia College (SC)                         |                  | ['https://www.columbiasc.edu/'] |
|  8 | US               | United States | ['clpccd.edu']     | Chabot-Las Positas Community College District |                  | ['http://www.clpccd.edu/']      |
|  9 | US               | United States | ['keller.edu']     | Keller Graduate School of Management          |                  | ['https://www.keller.edu/']     |
```

## Initiating Transform

### Output:

```
Transforming data...
```

## Data Transformed

### Output:

```
|    | unique_id                                                   | name                                          | country       | country_code   | state_province   | domains        | web_pages                   |
|---:|:------------------------------------------------------------|:----------------------------------------------|:--------------|:---------------|:-----------------|:---------------|:----------------------------|
|  0 | Marywood University_United States                           | Marywood University                           | United States | US             |                  | marywood.edu   | http://www.marywood.edu     |
|  1 | Lindenwood University_United States                         | Lindenwood University                         | United States | US             |                  | lindenwood.edu | http://www.lindenwood.edu/  |
|  2 | Sullivan University_United States                           | Sullivan University                           | United States | US             |                  | sullivan.edu   | https://sullivan.edu/       |
|  3 | Florida State College at Jacksonville_United States         | Florida State College at Jacksonville         | United States | US             |                  | fscj.edu       | https://www.fscj.edu/       |
|  4 | Xavier University_United States                             | Xavier University                             | United States | US             |                  | xavier.edu     | https://www.xavier.edu/     |
|  5 | Tusculum College_United States                              | Tusculum College                              | United States | US             |                  | tusculum.edu   | https://home.tusculum.edu/  |
|  6 | Claremont School of Theology_United States                  | Claremont School of Theology                  | United States | US             |                  | cst.edu        | https://cst.edu/            |
|  7 | Columbia College (SC)_United States                         | Columbia College (SC)                         | United States | US             |                  | columbiasc.edu | https://www.columbiasc.edu/ |
|  8 | Chabot-Las Positas Community College District_United States | Chabot-Las Positas Community College District | United States | US             |                  | clpccd.edu     | http://www.clpccd.edu/      |
|  9 | Keller Graduate School of Management_United States          | Keller Graduate School of Management          | United States | US             |                  | keller.edu     | https://www.keller.edu/     |
```

## Initiating SQL Query

### Output:

```
Executing SQL query on Spark DataFrame.
```

## SQL Query Executed

### Query:
```
SELECT name, country, state_province FROM universities WHERE state_province IS NOT NULL
```

### Output:

```
|    | name                                         | country       | state_province   |
|---:|:---------------------------------------------|:--------------|:-----------------|
|  0 | University of Pittsburgh Medical Center      | United States | Pennsylvania     |
|  1 | University of Pittsburgh Medical Center      | United States | Pennsylvania     |
|  2 | The University of Texas at Rio Grande Valley | United States | Texas            |
|  3 | Utah Valley University                       | United States | Utah             |
|  4 | Florida Polytechnic University               | United States | Florida          |
|  5 | College of Southern Nevada                   | United States | NV               |
|  6 | Bloomsburg University of Pennsylvania        | United States | Pennsylvania     |
|  7 | California University of Pennsylvania        | United States | Pennsylvania     |
|  8 | Clarion University of Pennsylvania           | United States | Pennsylvania     |
|  9 | Allen College                                | United States | Iowa             |
```

