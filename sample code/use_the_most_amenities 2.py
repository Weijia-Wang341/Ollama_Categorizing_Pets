from langchain_community.llms import Ollama
llm = Ollama(model="llama2")
import pandas as pd


df = pd.read_csv('Residency Reshaped_September 4, 2024_10.55.csv')
df = df['Q41']
df = df.iloc[2:]

unique_amenities = df.unique()

def hop(start, stop, step):
    for i in range(start, stop, step):
        yield i
    yield stop

index_list = list(hop(0, len(unique_amenities), 30))

def categorize_amenities(amenities, llm):
    response = llm.invoke("Can you format an appropriate form to the following answer on amenities. For example: Gym parking lot pool - gym, parking lot, pool; Don't have any - NA; This includes amenities like courtyards, rooftop terraces or garden areas for relaxation and socializing. - courtyards, rooftop terrace, garden; Rooftop Lounge, Fitness Center, Laundry Room - rooftop lounge, gym, laundry" + amenities)
    response = response.split('\n')
    print(response)

    ## in dataframe
    categories_df = pd.DataFrame({"use_the_most_amenities": response})
    categories_df[['input', 'cleaned']] = categories_df['use_the_most_amenities'].str.split(' - ', expand=True)

    return categories_df

##test case
categorize_amenities("People now prioritize safety above everything else, which makes them prefer residences with more security features, including gated communities or structures with restricted access; I work out at the fitness center a lot. For me, the parking lot is also quite important, and I frequently utilize the washing facilities;Swimming pool, bathtub and a big terrac;I often take advantage of the rooftop deck for its leisurely views and tranquility. My other favorite place to relax and mingle is the community lounge. Another popular place to have coffee and catch up on work or reading is the on site cafe.;Laundry Room, Bike Storage, Community Garden")