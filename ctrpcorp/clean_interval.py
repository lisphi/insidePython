def load_event_id():
    event_id_list = []
    with open('input/event_id.txt', 'r') as f:
        for line in f:
            event_id_list.append(line.strip())
    return event_id_list


def main():
    commands = []
    event_id_list = load_event_id()
    for i in range(0, len(event_id_list), 100):
        commands.append( { "eventIds": ",".join(event_id_list[i:i+100])})

    with open('ctrpcorp/output/clean_interval.txt', 'w') as f:
        for command in commands:
            f.write(str(command) + '\n')

    
if __name__ == '__main__':
    main()



