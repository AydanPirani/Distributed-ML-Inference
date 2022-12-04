import numpy as np

num_images = 5

c = 3
h = 224
w = 224

f = open("input1.dat", "w+")

for i in range(num_images):
    print(i)
    rand = np.random.rand(c * h * w)
    final = np.insert(rand, 0, [3, c, h, w])
    f.write(",".join(final.astype(str)))
    f.write("\n")

f.close()
    
