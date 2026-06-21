# 🚀 PySpark Core Concepts

<p align="center">
  <img src="https://img.shields.io/badge/PySpark-Core%20Concepts-orange?style=for-the-badge&logo=apachespark">
  <img src="https://img.shields.io/badge/Data%20Engineering-Learning-blue?style=for-the-badge">
  <img src="https://img.shields.io/badge/Language-Roman%20Urdu-green?style=for-the-badge">
</p>

---

# 📌 Introduction

PySpark Apache Spark ka Python API hai jo Big Data ko distributed environment mein process karta hai.

Spark ki sabse bari khasiyat hai:

* ⚡ Fast Processing
* 💾 In-Memory Computation
* 🔄 Fault Tolerance
* 🚀 Parallel Processing
* 📊 Scalability

Is README mein hum Spark ke important concepts Roman Urdu mein samjhenge.

---

# 📖 Table of Contents

* Lazy Evaluation
* In-Memory Computation
* Fault Tolerance
* Partition
* Repartition vs Coalesce
* Interview Questions

---

# ⚡ 1. Lazy Evaluation

## Definition

Lazy Evaluation ka matlab hai Spark transformations ko us waqt execute nahi karta jab aap code likhte hain.

Spark sirf execution plan banata hai.

Jab koi **Action** call hoti hai tab Spark saare transformations ko optimize karke execute karta hai.

---

## Example

```python
df = spark.read.csv("employees.csv")

df = df.filter(df.age > 25)

df = df.select("name", "salary")

df.show()
```

### Kya hua?

```text
Read CSV
     │
Filter
     │
Select
     │
Show (Action)
```

`show()` se pehle kuch bhi execute nahi hua.

---

## Transformations

Ye Lazy hoti hain.

```python
filter()

select()

join()

groupBy()

orderBy()

withColumn()

drop()

distinct()

union()
```

---

## Actions

Ye execution start karti hain.

```python
show()

count()

collect()

take()

first()

write()

foreach()
```

---

## Benefits

✅ Query Optimization

✅ Kam Disk I/O

✅ Better Performance

---

# 💾 2. In-Memory Computation

## Definition

Spark data ko baar baar disk se read nahi karta.

Data ko RAM mein store karke process karta hai.

Isi wajah se Spark Hadoop MapReduce se kaafi fast hai.

---

## Without In-Memory

```text
Disk Read

↓

Process

↓

Disk Write

↓

Disk Read

↓

Process
```

Har step mein Disk use ho rahi hai.

---

## With In-Memory

```text
Read Data

↓

Memory

↓

Filter

↓

Join

↓

Group By

↓

Output
```

Data RAM mein rehta hai.

---

## Cache Example

```python
df.cache()

df.count()

df.show()
```

Pehli action par data cache hoga.

Baad ki actions memory se chalengi.

---

## Benefits

⚡ Fast Processing

⚡ Less Disk Access

⚡ Better Performance

---

# 🔄 3. Fault Tolerance

## Definition

Agar cluster ka koi node fail ho jaye to Spark lost data ko recover kar leta hai.

Spark poora job dobara nahi chalata.

Sirf missing partition ko recompute karta hai.

---

## Example

```text
Node 1 → Partition 1

Node 2 → Partition 2

Node 3 → Partition 3 ❌

Node 4 → Partition 4
```

Node 3 fail ho gaya.

Spark Lineage use karega.

```text
CSV

↓

Filter

↓

Select

↓

Group By
```

Lost Partition dobara create ho jayega.

---

## Benefits

✔ Reliable Processing

✔ No Data Loss

✔ Automatic Recovery

---

# 📂 4. Partition

## Definition

Partition data ka chhota hissa hota hai.

Spark data ko multiple partitions mein divide karta hai taake parallel processing ho sake.

---

## Example

100 GB File

```text
+---------+
| Part 1 |
+---------+

+---------+
| Part 2 |
+---------+

+---------+
| Part 3 |
+---------+

+---------+
| Part 4 |
+---------+
```

Har partition alag executor process karega.

---

## Parallel Processing

```text
Executor 1 → Partition 1

Executor 2 → Partition 2

Executor 3 → Partition 3

Executor 4 → Partition 4
```

Sab ek saath kaam karte hain.

Isi liye Spark fast hai.

---

## Number of Partitions

```python
df.rdd.getNumPartitions()
```

---

# 🔀 Repartition

Partitions increase ya decrease karne ke liye.

```python
df = df.repartition(8)
```

✔ Shuffle hota hai.

✔ Equal Distribution hoti hai.

---

# 📉 Coalesce

Partitions reduce karne ke liye.

```python
df = df.coalesce(2)
```

✔ Shuffle avoid karta hai.

✔ Faster hota hai.

---

# 📊 Repartition vs Coalesce

| Repartition         | Coalesce               |
| ------------------- | ---------------------- |
| Increase + Decrease | Mostly Decrease        |
| Shuffle             | No Shuffle (Generally) |
| Slow                | Fast                   |

---

# 🎯 Interview Questions

## Q1. Lazy Evaluation kya hai?

Spark transformations ko immediately execute nahi karta.

Action aane par execute karta hai.

---

## Q2. In-Memory Computation kya hai?

Spark data ko RAM mein process karta hai.

Disk I/O kam hota hai.

Performance improve hoti hai.

---

## Q3. Fault Tolerance kya hai?

Node fail hone par Spark Lineage use karke lost partition recover karta hai.

---

## Q4. Partition kya hota hai?

Data ka smallest processing unit.

Har partition alag executor process karta hai.

---

## Q5. Repartition aur Coalesce mein difference?

Repartition shuffle karta hai.

Coalesce generally shuffle avoid karta hai.

---

# 📝 Summary

| Concept               | Purpose                                          |
| --------------------- | ------------------------------------------------ |
| Lazy Evaluation       | Execution ko delay karke optimize karna          |
| In-Memory Computation | RAM mein data process karke speed increase karna |
| Fault Tolerance       | Node failure ke baad recovery karna              |
| Partition             | Parallel processing ke liye data divide karna    |
| Repartition           | Partitions ko redistribute karna                 |
| Coalesce              | Partitions ko efficiently reduce karna           |

---

# 🎯 Final Note

Agar aap PySpark seekh rahe hain, to sabse pehle in concepts ko strong bana lijiye.

Ye concepts har Data Engineering interview mein frequently pooche jaate hain.

---

⭐ **Happy Learning!** 🚀
