from helpers import connect_to_sql, get_sql_con
from fastapi import FastAPI
import uvicorn
from sql_queries import get_sql_agg_most


def get_most():
    sql_query = get_sql_agg_most()

    df = connect_to_sql(sql_query=sql_query, sql_con=get_sql_con(
        database='testdbV2'), read_from_sql=True, return_df=True, return_df_as='json')

    return df


df = get_most()


app = FastAPI()


@app.get('/')
async def get_api_most():
    df = get_most()
    return df


if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=8000)
