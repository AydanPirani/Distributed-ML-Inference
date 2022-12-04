import tarfile
import urllib.request as urllib2
import urllib
from io import BytesIO


# rt = urllib2.urlopen("https://image-net.org/data/winter21_whole/n02352591.tar")
# csvzip = tarfile.open(fileobj=rt)
# import tarfile

# files_i_want = ['n02352591_5725.JPEG']


# tar = url.open("https://image-net.org/data/winter21_whole/n02352591.tar", mode='x:xz')
# tar.extractall(members=[x for x in tar.getmembers() if x.name in files_i_want])

# https://www.image-net.org/api/imagenet.attributes.obtain_synset_list

import urllib

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
    
for c in classes:
    x = urllib2.urlopen('https://image-net.org/data/winter21_whole/' + c + '.tar').read()
    tar = tarfile.open(fileobj=BytesIO(x))
    files_i_want = [tar.getmembers()[0].name, tar.getmembers()[1].name,tar.getmembers()[2].name,tar.getmembers()[3].name,tar.getmembers()[4].name, tar.getmembers()[5].name,tar.getmembers()[6].name,tar.getmembers()[7].name, tar.getmembers()[8].name, tar.getmembers()[9].name]
    tar.extractall(path= "./images/pics.txt", members=[x for x in tar.getmembers() if x.name in files_i_want])
    # tar.extractall(path= "./images", members=[x for x in tar.getmembers() if x.name == tar.getmembers()[2].name])


# x = urllib2.urlopen('https://image-net.org/data/winter21_whole/n02352591.tar').read()
# tar = tarfile.open(fileobj=BytesIO(x))
# tar.extractall(members=[x for x in tar.getmembers() if x.name == tar.getmembers()[2].name])
