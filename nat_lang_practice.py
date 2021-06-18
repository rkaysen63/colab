# Code to get PoS tags
import nltk
from nltk import word_tokenize
text = word_tokenize("I enjoy biking on the trails")
output = nltk.pos_tag(text)
print(output)
# Skill drill: Go ahead and run this code with a sentence of your choice.
text2 = word_tokenize("Go ahead and run this code with a sentence of your choice.")
output2 = nltk.pos_tag(text2)
print(output2)