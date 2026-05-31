import matplotlib.pyplot as plt

x = [1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256]
y_nn_all = [
    [0.010, 0.027, 0.026, 0.048, 0.13, 0.17, 0.26, 0.33, 0.79, 1.23, 1.71, 2.8, 5.19],
    [0.010, 0.014, 0.038, 0.099, 0.14, 0.14, 0.23, 0.33, 0.80, 1.21, 1.71, 2.8, 5.21],
    [0.010, 0.017, 0.024, 0.055, 0.147, 0.150, 0.269, 0.302, 0.818, 1.279, 1.779, 2.84, 5.11],
]

y_nn = []
for i in range(len(x)):
    sum = 0
    for batch in y_nn_all:
        sum += batch[i]
    y_nn.append(sum / len(y_nn_all))

y_insert_all = [
    [4371, 6432, 8679, 11905, 15112, 16922, 20295, 23919, 60620, 129851, 284151, 896723, 3036620],
    [4140, 6418, 8616, 11301, 13950, 17050, 19678, 22653, 53650, 115042, 279614, 908897, 3096490],
    [4088, 6486, 8797, 11412, 13939, 16823, 19486, 22419, 58399, 117762, 284905, 877268, 3077530],
]

y_insert = []
for i in range(len(x)):
    sum = 0
    for batch in y_insert_all:
        sum += batch[i]
    y_insert.append(sum / len(y_insert_all))

plt.figure(1)
plt.plot(x, y_nn, color='green', marker='o', linestyle='--')
plt.title("Зависимость времени NN запроса от размерности пр-ва")
plt.xlabel("размерность пространства, d")
plt.ylabel("Время NN запроса, ms")

plt.figure(2)
plt.plot(x, y_insert, color='red', marker='o', linestyle='--')
plt.title("Зависимость времени Insert запроса от размерности пр-ва")
plt.xlabel("размерность пространства, d")
plt.ylabel("Время Insert запроса, ns")

plt.show()
