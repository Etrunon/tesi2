import os

path = os.getcwd()
print(path)
path = path.split("Tesi2")[0]
print(path)
path += "Tesi2/"
print(path)

name = "total.csv"
# Legge un file.
input_file = open(path + "RunData/Input/" + name, "r")

# Scrive un file.
doubled_file_name = path + "RunData/Input/" + "TmpProcessed_" + name
doubled_file = open(doubled_file_name, "w")

for line in input_file:
    # print(line)
    line2 = line.split(", ")
    print(line2)
    line2 = line2[1].strip() + ", " + line2[0]

    doubled_file.write(line.strip() + "\n")
    doubled_file.write(line2 + "\n")

print("Lati raddoppiati")

input_file.close()
doubled_file.close()

doubled_file_read = open(doubled_file_name, "r")
output_file = open(path + "RunData/Input/" + "processed_" + name, "w")

all_lines = []
for line in doubled_file_read:
    all_lines.append(line)

print("Array in memoria")

all_lines.sort()

print("Array ordinato")
print("Numero lati: " + str(len(all_lines)))
final_lines = []
doppi = 0
for i in range(0, len(all_lines) - 1):
    if all_lines[i] == all_lines[i + 1]:
        print("uno eliminato: " + all_lines[i])
        doppi += 1
        pass
    else:
        final_lines.insert(0, all_lines[i])

print("Rimossi duplicati " + str(doppi) + "( " + str(len(all_lines)) + "  " + str(len(final_lines)) + " )")

for line in final_lines:
    output_file.write(line)

print("Profit")

doubled_file_read.close()
output_file.close()
