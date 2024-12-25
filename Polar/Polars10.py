import polars as pl
import pandas as pd
import re
from collections import Counter
import os


# Define the CSV file path

csv_file = "C:/Users/MuraliKrishnaKirthi/VS_Repository-1/.vscode/VS_Repository/Polar/ticket_data.csv"
df = pl.read_csv(csv_file, encoding='ISO-8859-1')

with open("C:/Users/MuraliKrishnaKirthi/VS_Repository-1/.vscode/VS_Repository/Polar/common_words.txt", 'r') as f:
    common_words = [line.strip() for line in f.readlines()]

all_words = []

short_description = df.select("Short Description").to_numpy().flatten()
# Calculate the count of each word using the Counter
word_counts = Counter(short_description)


# Create a DataFrame to show each word and its count
word_count_df = pl.DataFrame({
    "Word": list(word_counts.keys()),
    "Count": list(word_counts.values())
})

word_count_df = word_count_df.sort("Count", descending=True)

#***********************************************************************************************************************************
#*****************************SINGLE WORD EXTRACTION************************************************************************
#***********************************************************************************************************************************
filtered_words = []

for desc in short_description:
    words = re.findall(r'[A-Za-z0-9-]+|[^\w\d\s]', desc)
    all_words.extend(words)

# Create DataFrame for filtered words count
for word in all_words:
    if word not in common_words and not word.isdigit() and len(word) > 2:
        filtered_words.append(word)

filtered_word_counts = Counter(filtered_words)

filtered_word_counts_df = pl.DataFrame({
    "Word": list(filtered_word_counts.keys()),
    "Count": list(filtered_word_counts.values())
})

filtered_word_counts_df = filtered_word_counts_df.sort("Count", descending=True)

#***********************************************************************************************************************************
#***********************************BI-WORDs EXTRACTION******************************************************************
#***********************************************************************************************************************************


filtered_biwords = []
 
for desc in short_description:
    # Split the description into words
    words = re.findall(r'[A-Za-z0-9-%]+|[^\w\d\s]', desc)
    
    
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i + 1]}"  # Create a bigram (two consecutive words)

        # Filter out common words, digits, and short words (length <= 2)
        if (words[i] not in common_words and words[i + 1] not in common_words 
            and not words[i].isdigit() and not words[i + 1].isdigit() 
            and len(words[i]) > 2 and len(words[i + 1]) > 2):
            filtered_biwords.append(bigram)  # Add the valid bigram to the list

# Count the occurrences of each bigram using Counter
biword_counts = Counter(filtered_biwords)
biword_counts = pl.DataFrame({
    "Biword": list(biword_counts.keys()),
    "Count": list(biword_counts.values())
})

Biword_count_df = biword_counts.sort("Count", descending=True)


#***********************************************************************************************************************************
#***********************************TRI-WORDs EXTRACTION******************************************************************
#***********************************************************************************************************************************


filtered_triwords = []
 
for desc in short_description:
    # Split the description into words
    words = re.findall(r'[A-Za-z0-9-%]+|[^\w\d\s]', desc)
    
    
    for i in range(len(words) - 2):
        trigram = f"{words[i]} {words[i + 1]} {words[i + 2]}"  # Create a trigram (Three consecutive words)

        if (words[i] not in common_words and words[i + 1] not in common_words and words[i + 2] not in common_words 
            and not words[i].isdigit() and not words[i + 1].isdigit() and not words[i + 2].isdigit() 
            and len(words[i]) > 2 and len(words[i + 1]) > 2 and len(words[i + 2]) > 2):
            
            filtered_triwords.append(trigram)  # Add the valid trigram to the list
            
            

Triword_counts = Counter(filtered_triwords)
Triword_counts = pl.DataFrame({
    "Triword": list(Triword_counts.keys()),
    "Count": list(Triword_counts.values())
})

Triword_count_df = Triword_counts.sort("Count", descending=True)




#***********************************************************************************************************************************
#***********************************FOUR-WORDs EXTRACTION******************************************************************
#***********************************************************************************************************************************


filtered_fourwords = []

for desc in short_description:
    words = re.findall(r'[A-Za-z0-9-%]+|[^\w\d\s]', desc)
    words = [word.lower() for word in words]

    for i in range(len(words) - 3):
        fourgram = f"{words[i]} {words[i + 1]} {words[i + 2]} {words[i + 3]}"  # Create a fourgram (four consecutive words)


        if (words[i] not in common_words and words[i + 1] not in common_words and words[i + 2] not in common_words and words[i + 3] not in common_words
            and not words[i].isdigit() and not words[i + 1].isdigit() and not words[i + 2].isdigit() and not words[i + 3].isdigit()
            and len(words[i]) > 2 and len(words[i + 1]) > 2 and len(words[i + 2]) > 2 and len(words[i + 3]) > 2):
            filtered_fourwords.append(fourgram)



fourword_counts = Counter(filtered_fourwords)


fourword_counts_df = pl.DataFrame({
    "Fourword": list(fourword_counts.keys()),
    "Count": list(fourword_counts.values())
})


fourword_count_df = fourword_counts_df.sort("Count", descending=True)


#***********************************************************************************************************************************
#***********************************FIVE-WORDs EXTRACTION******************************************************************
#***********************************************************************************************************************************
filtered_fivewords = []

for desc in short_description:
    words = re.findall(r'[A-Za-z0-9-%]+|[^\w\d\s]', desc)
    words = [word.lower() for word in words]

    for i in range(len(words) - 4):
        fivegram = f"{words[i]} {words[i + 1]} {words[i + 2]} {words[i + 3]} {words[i + 4]}"


        if (words[i] not in common_words and words[i + 1] not in common_words and words[i + 2] not in common_words and words[i + 3] not in common_words
            and words[i + 4] not in common_words
            and not words[i].isdigit() and not words[i + 1].isdigit() and not words[i + 2].isdigit() and not words[i + 3].isdigit() and not words[i + 4].isdigit()
            and len(words[i]) > 2 and len(words[i + 1]) > 2 and len(words[i + 2]) > 2 and len(words[i + 3]) > 2) and len(words[i + 4]) > 2:
            filtered_fivewords.append(fivegram)



fiveword_counts = Counter(filtered_fivewords)


fiveword_counts_df = pl.DataFrame({
    "Fiveword": list(fiveword_counts.keys()),
    "Count": list(fiveword_counts.values())
})


fiveword_count_df = fiveword_counts_df.sort("Count", descending=True)

#*****************************************************************************************************
output_csv_file = "C:/Users/MuraliKrishnaKirthi/VS_Repository-1/.vscode/VS_Repository/Polar/Output/0_Description_Count.csv"
output_csv_file1 = "C:/Users/MuraliKrishnaKirthi/VS_Repository-1/.vscode/VS_Repository/Polar/Output/1_SingleWord_Count.csv"
output_csv_file2 = "C:/Users/MuraliKrishnaKirthi/VS_Repository-1/.vscode/VS_Repository/Polar/Output/2_BIWord_Count.csv"
output_csv_file3 = "C:/Users/MuraliKrishnaKirthi/VS_Repository-1/.vscode/VS_Repository/Polar/Output/3_TRIWord_Count.csv"
output_csv_file4 = "C:/Users/MuraliKrishnaKirthi/VS_Repository-1/.vscode/VS_Repository/Polar/Output/4_FOURWord_Count.csv"
output_csv_file5 = "C:/Users/MuraliKrishnaKirthi/VS_Repository-1/.vscode/VS_Repository/Polar/Output/5_FiveWord_Count.csv"

word_count_df.write_csv(output_csv_file)
filtered_word_counts_df.write_csv(output_csv_file1)
Biword_count_df.write_csv(output_csv_file2)
Triword_count_df.write_csv(output_csv_file3)
fourword_count_df.write_csv(output_csv_file4)
fiveword_count_df.write_csv(output_csv_file5)

print(f"Results saved to output_csv_files")

#*****************************************************************************************************