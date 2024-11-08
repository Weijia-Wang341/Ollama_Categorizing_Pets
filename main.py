from langchain_community.llms import Ollama
import pandas as pd

llm = Ollama(model='mistral')

df = pd.read_csv('sample data/input data/pet.csv')
df['pet'] = df['pet'].astype(str)

unique_transaction = df['pet'].unique()

def hop(start, stop, step):
    for i in range(start, stop, step):
        yield i
    yield stop

index_list = list(hop(0, len(unique_transaction), 30))

def categorize_transactions(breed, llm):
    response = llm.invoke('can you add an appropriate category next to each of the following andmal or breed names. for example: british long hair - cat, golden - dog, gold fish - fish, parrot - bird' + breed)
    response = response.split('\n')
    

    categories_df = pd.DataFrame({'animals vs category': response})
    categories_df[['animals', 'category']] = categories_df['animals vs category'].str.split(' - ', expand=True)

    return categories_df


from pydantic import BaseModel, field_validator
from typing import List

class FormatError(Exception):
    pass

# Validate response format - check if it actually contains hyphen ("-")
class ResponseChecks(BaseModel):
    data: List[str]

    @field_validator("data")
    def check(cls, value):
        for item in value:
            if len(item) > 0:
                assert "-" in item, "String does not contain hyphen."
        return value

# Test validation
ResponseChecks(data = ['Hello - World', 'Hello - there!'])


categories_df_all = pd.DataFrame()
max_tries = 30


def process_transactions(breed, llm):
    for attempt in range(max_tries):
        try:
            categories_df = categorize_transactions(breed, llm)
            response_data = categories_df['amenities vs category'].tolist()
            ResponseChecks(data=response_data)
            return categories_df
        except FormatError as e:
            print(f"Attempt {attempt + 1} failed: {e}, retrying...")
            continue
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying...")
            continue
    raise Exception('Failed to categorize transactions after multiple attempts.')

for i in range(0, len(index_list)-1):
    amenities = unique_transaction[index_list[i]:index_list[i+1]]
    amenities = ','.join(amenities)

    try:
        categories_df = categorize_transactions(amenities, llm)
        categories_df_all = pd.concat([categories_df_all, categories_df], ignore_index=True)

    except Exception as e:
        print(f"final failure for transaction indes {i} to {i+1}: {e}")
categories_df_all.to_csv('sample data/output data/animals_category.csv', index=False)



