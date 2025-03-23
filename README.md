# DBL-group-13

Run **data_loading_w_sentiment.py** to create a SQL database of the data that is placed in /test_data/ folder. It immediately applies sentiment analysis<br> 

Run **conversations.py** to add a INTERACTIONS table to the sql database <br>
Run **dutch_conversations.py** to add a DUTCH_INTERACTIONS table to the sql database <br>

Run **Sentiment Analysis.py** (and **dutch sentiment.py**) to add sentiment analysis columns, polarity and subjectivity to INTERACTIONS table
This is still necessary even if you used data_loading_w_sentiment. So that columns match up for older notebooks

    This also creates some SQL views. Of note is the:
        INTERACTIONS_WITH_TEXT view, the interactions table but with text columns that are expanded if necessary
        EXPANDED_INTERACTIONS, which has text columns and creation time columns

Run **add full conversations table.py** to add table of full conversations
Optionally: <br>
Run hashtags.py to create HASHTAGS table <br>
<br>
Other:


**Exploration.ipynb** contains some initial data exploration (sprint 1)

**Graphs and Plots.ipynb** contains the code to generate graphs and plots (sprint 1)

**Sentiment Analysis - Exploration and Comparisons.ipynb** graphs, plots and airline comparisons regarding sentiment (sprint 2/3)

**Extra Presentation 2.ipynb** plots specifically for use in presentation 2

**Geoplots.ipynb** notebook for geoplots, since these can cause a notebook to become very slow we keep them mostly seperate

**get full conversations.ipynb**, for initial research into getting full conversations

**Word analysis**, for initial research into analysis for the word cloud

**Poster WIP plots**, for code demonstration of most poster plots


