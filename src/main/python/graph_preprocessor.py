import os

path = os.getcwd()
print(path)
path = path.split("Tesi2")[0]
print(path)
path = path + "Tesi2/"
print(path)

name = "mini1.csv"
# Legge un file.
in_file = open(path + "RunData/Input/" + name, "r")

# Scrive un file.
out_file = open(path + "RunData/Input/" + "processed_" + name, "w")

for line in in_file:
    # print(line)
    line2 = line.split(", ")
    print(line2)
    line2 = line2[1].strip() + ", " + line2[0]

    out_file.write(line.strip() + "\n")
    out_file.write(line2 + "\n")

in_file.close()
out_file.close()
