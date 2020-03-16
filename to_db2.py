# SQLite3をインポート
import sqlite3
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymongo
from pymongo import MongoClient
import os
import glob

client = MongoClient('localhost', 27017)

db = client.mydb


def output_sql():
    s = pd.Series(np.random.randn(50))
    s.head()
    df = pd.DataFrame(np.random.randn(50, 4), columns=list('ABCD'))
    df.head()

    engine = create_engine("mysql://tomoaki_hori:zgmfx09aw000@localhost/ada?unix_socket=/var/run/mysqld/mysqld.sock")
    df = pd.DataFrame(['A', 'B'], columns=['new_tablecol'])
    df.to_sql(name='new_table', con=engine, if_exists='append')


def output_mongo():
    s = pd.Series(np.random.randn(50))
    s.head()
    df = pd.DataFrame(np.random.randn(50, 4), columns=list('ABCD'))

    db.something.insert_many(df.to_dict(orient="records"))


def input_mongo():
    # create a dictionary for the DataFrame frame dict
    df_series = []
    for num, series in enumerate(db.something.find()):
        # same as: df_series["data 1"] = series
        df_series.append(series)
    pd.DataFrame(df_series)


"""
Agenda
1. 生データの再現データを作成
つまり、レコード名、測定日、プロット名、値（9個）からなるデータを２つ作成
2. 測定項目に関わるデータを作成
項目は項目名、単位、データ型、scaling、注釈（今回はhogeとfuga）
これは一枚で良い
3. データコレクターの出力順に対応するデータを作成
項目はデータ名、項目名
4. 1のレコード名に応じて入力するデータベースを変える
SP、TI、PLなど
5. 各データベースに対して
    1. 2に応じてinputデータに名前付け
    2.  2, 3を結合（DBの機能を使えると尚良.left_joinかな？）
    3. 5-2を基に値のスケーリングを実行
    3.5. 5-2を基にデータの型を確認?
    4. 3をJSON状に変更。
    5. overlap舌値は
6. 各データベースの値を全て出力。
7. DBごとだけでなく、各DBを結合したものもおく
"""


def setting_db():
    sample_number = 4
    # 1. 生データの再現データを作成
    data1 = pd.DataFrame({'idx': list(np.tile(1, 5)),
                          'date': list(np.tile("2017/03/01", 5)),
                          'time': ["13:01", "13:01", "13:02", "13:03", "13:05"],
                          'item_identifier': np.tile("pl", 5),
                          'plot_num': [1, 2, 3, 4, 5],
                          1: np.random.randint(100, 200, 5),
                          2: np.random.randint(100, 200, 5),
                          3: np.random.randint(100, 200, 5),
                          4: np.random.randint(100, 200, 5),
                          })

    data2 = pd.DataFrame({'idx': list(np.tile(2, 6)),
                          'date': ["2017/03/01" for i in range(6)],
                          'time': ["13:01", "13:01", "13:02", "13:03", "13:05", "13:08"],
                          'item_identifier': np.tile("sp", 6),
                          'plot_num': [1, 2, 3, 4, 5, 5],
                          "hoge": ["fuga" for i in range(6)],
                          1: np.random.uniform(100, 300, 6),
                          2: np.random.uniform(100, 200, 6),
                          3: np.random.uniform(100, 200, 6),
                          4: np.random.uniform(100, 200, 6),
                          })

    # 測定項目に関わるデータを作成
    # 項目は項目名、単位、データ型、scaling、注釈（今回はhogeとfuga）
    # これは一枚で良い
    iname = ["date", "time", "plot_num", "sample_num", "PL", "SP", "TI", "hoge"]
    exp_item = pd.DataFrame({"item_name": ["date", "time", "plot_num", "sample_num", "PL", "SP", "TI", "fuga"],
                             "order": ["yyyy/mm/dd", "hh:mm", None, None, "cm", None, "number/hill", None],
                             "data_type": ["str", "str", "int", "int", "float", "float", "int", None],
                             "scale": [None, None, 1, 1, 0.1, 0.1, 1, None],
                             "collection_name": [None, None, None, None, "plant_hei", "spa", "til", "fugas"]})
    # db.exp_item.create_index("item_name", unique=True)

    for dicti in exp_item.to_dict(orient="records"):
        db.exp_item.update_one(dicti, {"$set": dicti}, upsert=True)

    # 3. データコレクターの出力順に対応するデータを作成
    # 項目はデータ名、項目名
    log_pl = pd.DataFrame({"log_name": ["date", "time", "plot_num"] + list(range(sample_number)),
                           "item_name": ["date", "time", "plot_num"] + ["PL" for i in range(sample_number)],
                           "input_order": np.arange(7),
                           "index_name": [None for i in range(3)] + ["sample_name" for i in range(sample_number)],
                           "index_number": [None for i in range(3)] + list(range(sample_number))
                           }
                          )

    log_sp = pd.DataFrame({"log_name": ["date", "time", "plot_num", "fuga"] + list(range(4)),
                           "item_name": ["date", "time", "plot_num", "fuga"] + ["SP" for i in range(sample_number)],
                           "input_order": np.arange(8),
                           "index_name": [None for i in range(4)] + ["sample_name" for i in range(sample_number)],
                           "index_number": [None for i in range(3)] + ["fuga"] + list(range(sample_number))
                           }
                          )

    # for dicti in log_pl.to_dict(orient="records"):
    #     db.log_pl.update_one(dicti, {"$set": dicti}, upsert=True)
    # for dicti in log_sp.to_dict(orient="records"):
    #     db.log_sp.update_one(dicti, {"$set": dicti}, upsert=True)

    # db.log_sp.insert_many(log_sp.to_dict(orient="records"))
    # db.log_pl.delete_many({})
    # db.log_sp.delete_many({})

    # item_identifierとデータベースの対応表
    # item_identifierが〜、collection_nameが〜
    db_name_correspo = "corresponding_log_identifier"
    correspo = pd.DataFrame({"item_identifier": ["sp", "pl", "ti"],
                             "collection_name": ["log_sp", "log_pl", "log_ti"]})
    correspo_d = correspo.to_dict(orient="record")
    correspo_d[0]['collection'] = log_sp.to_dict(orient='record')
    correspo_d[1]['collection'] = log_pl.to_dict(orient='record')

    for correspo_di in correspo_d:
        db[db_name_correspo].update_one({"collection_name": correspo_di["collection_name"]},
                                        {"$set": correspo_di}, upsert=True)

    # 4. 4. 1のレコード名に応じて入力するデータベースを変える
    # records_pre = data1.to_dict(orient="records") + data2.to_dict(orient="records")
    # records = [[i for i in fi.values()] for fi in records_pre]

    # data1, 2をcsvに保存してtxtとして結合
    dir_out = "analysis/mdb_test_out"
    data1.to_csv(f"{dir_out}/data1.csv", index=False, header=False)
    data2.to_csv(f"{dir_out}/data2.csv", index=False, header=False)
    read_files = glob.glob(f"{dir_out}/*.csv")

    with open(f"{dir_out}/data.txt", "wb") as outfile:
        for f in read_files:
            with open(f, "rb") as infile:
                outfile.write(infile.read())


def mongod_trial(infile, dir_out="analysis/mdb_test_out"):
    # dbの中にあるリストを結合
    pipeline = [
        {
            "$lookup": {
                "from": "exp_item", "localField": "item_name", "foreignField": "item_name", "as": "exp_item"
            }
        },
        {"$unwind": "$exp_item"},
        { "$match":{'item_identifier': 'pl'}},
        {
            "$project": {"collection._id": 1, "collection.log_name": 1, "collection.item_name": 1, "collection.input_order": 1, "data_type": "$exp_item.data_type",
                         "collection.scale": "$exp_item.scale", "collection.index_name": 1,
                         "collection_name": "$exp_item.collection_name", "collection.index_number": 1,
                         }
        }]

    loc_identifier = 3

    # データ構築に必要なりすとなど
    db_name_correspo = "corresponding_log_identifier"
    check_columns = ["date", "plot_num"]
    sorted_columns_pre = ["date", "time", "plot_num"]

    # 考えられる識別子を全て列挙
    n_identify = pd.DataFrame([k for k in db[db_name_correspo].find({}, projection={'item_identifier': 1})]).iloc[:,
                 1].values

    records = []  # as you want these as your headers
    with open(infile) as f:
        for line in f:
            # remove whitespace at the start and the newline at the end
            line = line.strip()
            # split each column on whitespace
            columns = line.split(',')
            records.append(columns)

    for ki in n_identify:
        li = list()
        for i in records:
            if i[loc_identifier] == ki:
                # 必要な部分のみ抜き出し
                li.append([i[k] for k in [1, 2] + list(range(4, len(i)))])

        if len(li) > 0:
            ldbi = [k for k in db[db_name_correspo].find({'item_identifier': ki})][0]['collection_name']
            dbi_item = pd.DataFrame([doc for doc in (db[ldbi].aggregate(pipeline))]
                                    ).sort_values(by="input_order")
            # もしto_be_separatedがlogに含まれているならばデータを分割する

            # recordをデータフレームに変換
            # このままでは全てstrのままなので、collectionに従って型を変換する
            dbi_df = pd.DataFrame(li, columns=dbi_item.log_name
                                  ).convert_objects(convert_numeric=True)
            scale_pos_dbi = dbi_item.log_name.loc[~dbi_item.scale.isnull()]
            # #collectionのscaleのとおりに入力値の桁を調整
            dbi_df.loc[:, scale_pos_dbi] = dbi_df.loc[:, scale_pos_dbi].apply(
                lambda x: x * dbi_item.scale.loc[~dbi_item.scale.isnull()].values, axis=1)
            dbi_df.plot_num = dbi_df.plot_num.astype(int)

            # 複数値を取得する項目について、サンプル番号をふり、データをスタックする
            pivoted_value_index = dbi_item.log_name.loc[~dbi_item.index_number.isnull()].values
            sample_value_index = dbi_item.log_name.loc[dbi_item.index_number.isnull()].values
            dbi_stacked_pre = dbi_df.melt(id_vars=sample_value_index, value_vars=pivoted_value_index)
            dbi_stacked_pre2 = pd.merge(dbi_stacked_pre,
                                        dbi_item.loc[:,
                                        ["log_name", "collection_name", "item_name", "index_name", "index_number"]],
                                        on=["log_name"], how="left")

            # データ取得時刻に合わせてデータを並べ替え
            sampling_datetime = pd.to_datetime(
                dbi_stacked_pre2.date.astype(str) + ' ' + dbi_stacked_pre2.time.astype(str),
                format='%Y/%m/%d %H:%M')
            dbi_stacked = dbi_stacked_pre2.loc[sampling_datetime.sort_values().index]

            for itemi in dbi_stacked.item_name.unique():
                dbi_stacked_i = dbi_stacked.loc[dbi_stacked.item_name.str.match(itemi)]
                # saveするコレクションの名前
                collection_name_i = dbi_stacked_i.collection_name.unique()[0]
                # 複数値を取得する項目について、サンプル番号に対応する列名を降る
                # 値を一つしか取らないないならばここの値は空になる
                index_name = dbi_stacked_i.index_name.unique()[0]

                # データベースへのコミット。ここで以前のデータのチェックを実行
                # 以前と日付、plot_name, sample_nameについて全く同じデータがデータベース上にすでにあるならば
                # 計測時間が遅いものを優先してプッシュする
                if index_name is None:
                    # 各レコードのユニーク性を担保する列
                    check_columnsj = check_columns
                    # データフレームを見やすく並べ替えるための列名
                    sorted_columns = sorted_columns_pre + ["collection_name", "item_name", "value"]

                    dbi_stacked_i_use = dbi_stacked_i.drop(
                        ["index_name", "log_name", "index_number", "collection_name"], axis=1).loc[:, sorted_columns]

                else:
                    check_columnsj = check_columns + [index_name]
                    sorted_columns = sorted_columns_pre + [index_name, "collection_name", "item_name", "value"]
                    dbi_stacked_i_use = dbi_stacked_i.rename(
                        {"index_number": index_name}, axis=1).drop(
                        ["index_name", "log_name", "collection_name"], axis=1).loc[:, sorted_columns]

                # 既存のレコードとの重複をチェック
                tb_inserted = dbi_stacked_i_use.to_dict(orient="record")
                find_duplicated_idx = {"$or":
                                           [{"$and": [{k: tb_inserted_j[k]} for k in check_columnsj]}
                                            for tb_inserted_j in tb_inserted]}
                duplicated1_pre = db[collection_name_i].find(find_duplicated_idx)
                duplicated1 = pd.DataFrame([i for i in duplicated1_pre])
                if duplicated1.shape[0] > 0:
                    print("#" * 8 + f"Input data is duplicated ~~ collection '{collection_name_i}'" + "#" * 8)
                    print("#" * 8 + f"Following data is already updated." + "#" * 8)
                    print(duplicated1)
                    duplicated1.to_excel(f"{dir_out}/{collection_name_i}_duplicated.xlsx", index=False)

                # 入力データに日付、プロットおよびsample_nameに関して重複があればそれも記述しておく
                # 重複がある場合は後で記録した方を優先して使うようにする
                dbi_stacked_i_use["duplicated"] = dbi_stacked_i_use.duplicated(check_columnsj, keep="last")

                # オリジナルの結果を保存
                dbi_stacked_i_use.to_excel(f"{dir_out}/{collection_name_i}_org.xlsx", index=False)

                # 修正結果も反映可能なファイルを保存
                dbi_stacked_i_use.to_excel(f"{dir_out}/{collection_name_i}_modified.xlsx", index=False)


def updated_to_db(dir_out="analysis/mdb_test_out"):
    phenotype_collection = "phenotype"
    # データのアップデート
    list_files = glob.glob(f"{dir_out}/*_modified.xlsx")
    for filei in list_files:
        records_df_pre = pd.read_excel(filei)
        records_df = records_df_pre.loc[~records_df_pre['duplicated']].drop(["duplicated", "collection_name"],
                                                                            axis=1).to_dict(orient="record")
        column_use_pre = ["date", "plot_num", "item_name"]
        if "sample_name" in records_df_pre.columns:
            column_use = column_use_pre + ["sample_name"]
        else:
            column_use = column_use_pre

        # データのアップロード
        for rec_i in records_df:
            dic_pip = {"$and": [{k: rec_i[k]} for k in column_use]}
            db[phenotype_collection].update_one(dic_pip, {"$set": rec_i}, upsert=True)

    # 念のため、表現型データを出力
    result = pd.DataFrame([k for k in db[phenotype_collection].find()])
    result['_id'] = result['_id'].astype(str)
    result.to_excel(f"{dir_out}/phenotype.xlsx")


def main():
    # 入力のファイルの名前
    dir_out = "analysis/mdb_test_out"
    infile = f"{dir_out}/data.txt"
    setting_db()
    mongod_trial(infile=infile)
    updated_to_db()
