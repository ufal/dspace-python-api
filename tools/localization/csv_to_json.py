with open("cs.csv", encoding='utf-8') as file:
    with open("out.json", "w+", encoding="utf-8") as out_file:
        out_file.write("{\n")
        for line in file.readlines():
            print(f"parsing line {line}")
            sep = line.split(",")
            key = sep[0]
            val = "".join(sep[1:])
            if val[0] != '"':
                val = '"' + val
            if val[-1] == "\n":
                val = val[:-1]
            if val[-1] != '"':
                val = val + '"'
            val = val.replace('\\""', '\\"')
            print(f'"{key}": {val},', file=out_file)

        out_file.write("}\n")
