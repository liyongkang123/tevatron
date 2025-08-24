import json
import os
import pandas as pd
from datasets import load_dataset
from openpyxl.styles.builtins import output

def parse_line(line: str):
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    parts = s.split()
    # 允许 qid Q0 docid rel / qid docid rel / qid docid
    if len(parts) >= 4:
        qid, mid, docid, rel = parts[0], parts[1], parts[2], parts[3]
        if mid.upper() == "Q0":
            return str(qid), str(docid), str(rel)
        else:
            return str(qid), str(docid), str(rel)
    elif len(parts) == 3:
        qid, docid, rel = parts
        return str(qid), str(docid), str(rel)
    elif len(parts) == 2:
        qid, docid = parts
        return str(qid), str(docid), "1"
    return None

def transfer_qrels():
    # 两个 split 分开执行
    # qrels_evidence_path = 'topics-qrels/qrel_evidence.txt'
    # output_qrels_evidence_path = 'browsecomp_plus/qrels/evidence.tsv'
    qrels_evidence_path = 'topics-qrels/qrel_golds.txt'
    output_qrels_evidence_path = 'browsecomp_plus/qrels/golds.tsv'

    # 读取文件并转换成 beir格式
    total_in = total_out = 0
    with open(qrels_evidence_path, "r", encoding="utf-8") as fin, open(output_qrels_evidence_path, "w", encoding="utf-8") as fout:
        # 写表头
        fout.write("query-id\tcorpus-id\tscore\n")
        for line in fin:
            parsed = parse_line(line)
            if not parsed:
                continue
            qid, docid, rel = parsed
            fout.write(f"{qid}\t{docid}\t{rel}\n")
            total_in += 1
            total_out += 1
    print(f"完成：读取 {total_in} 行，写入 {total_out} 行到 {output_qrels_evidence_path}")

def transfer_corpus():
    data= load_dataset("Tevatron/browsecomp-plus-corpus",split="train" )
    output_corpus_path = 'browsecomp_plus/corpus.jsonl'

    # 遍历data 然后按行每行一个json 写入 output_corpus_path
    # 写入 jsonl 文件
    with open(output_corpus_path, "w", encoding="utf-8") as f:
        for item in data:
            new_json = {}
            new_json["_id"] = item["docid"]
            new_json["text"] = item["text"]
            new_json["title"] = item.get("title", "")
            new_json['metadata']={'url': item.get('url', '')}
            # 写入文件
            f.write(json.dumps(new_json, ensure_ascii=False) + "\n")

    print(f"已保存到 {output_corpus_path}，共 {len(data)} 行")

def transfer_queries():
    data_path = "data/browsecomp_plus_decrypted.jsonl"
    output_queries_path = "browsecomp_plus/queries.jsonl"
    ds = load_dataset("json", data_files=data_path, split="train")  # 单文件也可以用 train 这个名字
    print(ds)
    with open(output_queries_path, "w", encoding="utf-8") as f:
        for item in ds:
            new_json = {}
            new_json["_id"] = item["query_id"]
            new_json["text"] = item["query"]
            new_json['metadata']={ }
            # 写入文件
            f.write(json.dumps(new_json, ensure_ascii=False) + "\n")

    print(f"已保存到 {output_queries_path}，共 {len(ds)} 行")

def main():
    pass


if __name__ == "__main__":
    transfer_queries()