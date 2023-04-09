from datetime import datetime

def main():
    name = input('your name:')
    gender = input('your gender(male/female):')
    birthday = datetime(2011, 2, 11)

    str1 = "{0}'s birthday is on {1:%Y-%m-%d}, gender: {2}".format(name, birthday, gender)
    str2 = f"{name}'s birthday is on {birthday.strftime('%Y-%m-%d')}, gender: {gender}"
    print(str1)
    print(str2)

if __name__ == "__main__":
    main()