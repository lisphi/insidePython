import re
import os

def parse(text):
    # 使用正则表达式去除标点符号和换行符
    text = re.sub(r'[^\w ]', ' ', text)
 
    # 转为小写
    text = text.lower()
    
    # 生成所有单词的列表
    word_list = text.split(' ')
    
    # 去除空白单词
    word_list = filter(None, word_list)
    
    # 生成单词和词频的字典
    word_cnt = {}
    for word in word_list:
        if word not in word_cnt:
            word_cnt[word] = 0
        word_cnt[word] += 1
    
    # 按照词频排序
    sorted_word_cnt = sorted(word_cnt.items(), key=lambda kv: kv[1], reverse=True)
    
    return sorted_word_cnt

def main():
    with open(os.path.join(os.getcwd(), 'class_06_in.txt'), 'r') as fin:
        text = fin.read()

    for word, freq in parse(text):
        print(f"{word}: {freq}")

if __name__ == '__main__':
    main()


