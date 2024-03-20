# 裁判文书清洗

这是 [MNBVC](https://mnbvc.253874.net) 项目中裁判文书的清洗代码。清洗后的数据在 Hugging Face: [wormtooth/MNBVC-judgment](https://huggingface.co/datasets/wormtooth/MNBVC-judgment)

裁判文书网数据有两个来源，一份已经清洗过放在在huggingface，在2022年得到；另一份在百度网盘，2023年得到。在获取数据的过程中，数据中的一些字会变成乱码。乱码位置随机。以下是明心等人对乱码的总结：
- 乱码有时2位有时3位
- 被替换的自负有时是1个有时是2个
- 乱码个数和被替换的字个数无关

## 清洗任务
- [x] 合并重复的裁判文书
- [x] 将乱码还原成文字
- [x] 转化成通用语料格式

## 脚本顺序

脚本生产的 cache 达308GB，清洗后的数据有491GB，压缩后的最终数据有125GB。所以运行脚本前保证有1TB左右的存储容量。

**1_partition_by_case_id.py**: 将两份数据源的裁判文书按照案号分割成5个部分便于处理。

**2_process_hf_only_unq.py**: 处理只在 Hugging Face 源出现一次的裁判文书。

**3_process_hf_only_dup.py**: 处理只在 Hugging Face 源出现多次的裁判文书 - 去重并且将乱码还原。

**4.1_convert_bd_only_unq_to_db.py**: 将只在百度网盘源出现一次的裁判文书的案号先保存为 sqlite 数据库，便于查询。这是为了减少内存的使用，便于多进程处理。如果内存足够大（比如64G以上），可以使用类似 2_process_hf_only_unq.py 中的方法。

**4.2_process_bd_only_unq.py**: 处理只在百度网盘源出现一个的裁判文书。

**5.process_bd_only_dup.py**: 处理只在百度网盘源出现多次的裁判文书 - 去重并且将乱码还原。

**6.1_find_common_dup_cases.py**: 将同时在两个源存在的裁判文书存在 sqlite 数据库中。

**6.2_process_hf_bd_common.py**: 处理同时在两个源出现裁判文书 - 去重并且将乱码还原。

**7.compress.py**: 以 gz 格式压缩 jsonl。

**8.upload.py**: 上传数据到 Hugging Face: [wormtooth/MNBVC-judgment](https://huggingface.co/datasets/wormtooth/MNBVC-judgment)