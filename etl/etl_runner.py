from etl.loader import foodpang, neulpum

def run_all(config: dict):
    df1 = foodpang.load(config["foodpang_path"])
    df2 = neulpum.load(config["neulpum_path"])

    return df1, df2