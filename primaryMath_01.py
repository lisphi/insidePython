# 100以内加减法算术题生成器

import random
import tkinter as tk
from tkinter import messagebox

def generate_questions():
    question_list = []
    for _ in range(10):
        row = []
        for _ in range(4):
            valid_question = False
            while not valid_question:
                num1 = random.randint(1, 99)
                num2 = random.randint(1, 99)
                operation = random.choice(["+", "-"])
                if is_easy(num1, num2, operation): 
                    continue
                question = f"{num1:2}{operation}{num2:2}"
                answer = eval(question)
                if 0 <= answer <= 100:
                    valid_question = True
                    row.append(question)
        question_list.append(row)
    return question_list

def is_easy(num1, num2, operation):
    return "+" == operation and (num1 % 10 + num2 % 10 < 10) or "-" == operation and (num1 % 10 >= num2 % 10) 


def show_questions():
    questions = generate_questions()
    for i, row in enumerate(questions):
        for j, question in enumerate(row):
            question_labels[i][j].config(text=f"{question}=", fg="black")
            answer_labels[i][j].config(text="")

def show_answers():
    for i, row in enumerate(question_labels):
        for j, question_label in enumerate(row):
            question = question_label.cget("text")
            if question:
                answer = eval(question.rstrip("="))
                answer_labels[i][j].config(text=f"{answer:2}", fg="red")

root = tk.Tk()
root.title("小学生算术题")

frame = tk.Frame(root, padx=10, pady=10)
frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

question_font = ("Aril", 24)

question_labels = [[tk.Label(frame, font=question_font, width=10, anchor='e') for _ in range(4)] for _ in range(10)]
answer_labels = [[tk.Label(frame, font=question_font, width=4, anchor='w') for _ in range(4)] for _ in range(10)]

for i in range(10):
    for j in range(4):
        question_labels[i][j].grid(row=i, column=2 * j, sticky=tk.W)
        answer_labels[i][j].grid(row=i, column=2 * j + 1, sticky=tk.W)

btn_frame = tk.Frame(root, padx=10, pady=10)
btn_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

generate_btn = tk.Button(btn_frame, text="出题", command=show_questions)
generate_btn.grid(row=0, column=0, padx=10)

answer_btn = tk.Button(btn_frame, text="答案", command=show_answers)
answer_btn.grid(row=0, column=1, padx=10)

root.mainloop()
