import tarfile
import urllib.request as urllib2
import urllib
from io import BytesIO
from PIL import Image
import urllib
from numpy import asarray

# import cv2 
#pip install opencv-python


url = "https://www.image-net.org/api/imagenet.attributes.obtain_synset_list"
file = urllib. request. urlopen(url)
count = 0
classes = []
for line in file:
    if(count == 50):
        break
    decoded_line = line.decode("utf-8")
    classes.append(decoded_line.strip())
    count = count + 1
print(classes)
    
filenames = []
for c in classes:
    x = urllib2.urlopen('https://image-net.org/data/winter21_whole/' + c + '.tar').read()
    tar = tarfile.open(fileobj=BytesIO(x))
    files_i_want = [tar.getmembers()[0].name, tar.getmembers()[1].name,tar.getmembers()[2].name,tar.getmembers()[3].name,tar.getmembers()[4].name, tar.getmembers()[5].name,tar.getmembers()[6].name,tar.getmembers()[7].name, tar.getmembers()[8].name, tar.getmembers()[9].name]
    filenames.extend(files_i_want)
    tar.extractall(path= "./images/pics.txt", members=[x for x in tar.getmembers() if x.name in files_i_want])

# f = open("input1.txt", "w+")

with open("input1.txt", 'w+', encoding='utf-8') as my_file:
    for f in filenames:
        image = Image.open("./images/pics.txt/"+f)
        resized = image.resize((224, 224))
        resized.save(f)
        data = asarray(resized)
        my_file.write(",".join(map(str, data)))
        my_file.write("\n")
        # print("done")
# f.close()
