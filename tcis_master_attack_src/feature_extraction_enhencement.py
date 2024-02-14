import numpy as np

def feat_extract_enhence(df):
    adjust_cwd_val = 1 #  if setting 0, meaning "powershell's cwd is not considered"
    adjust_name_val = 5
    suspecious_cwd_cnt = 0
    suspecious_name_cnt = 0

    # def suspect_cwd(df):
    #     dev_path = df[df["Description"] == "powershell.exe"]["ExecutablePath"]
    #     suspecious_cwd_cnt = dev_path.apply(lambda x: True if (len(x.strip().split('\\')) > 3)
    #                                                           or ('Users' not in x.strip().split(
    #         '\\')) else False).sum()
    #     # print("suspecious_cwd_cnt", suspecious_cwd_cnt)
    #     return suspecious_cwd_cnt * adjust_cwd_val

    def suspect_name_spelling(df):
        suspect_name_spelling_cnt = df['Description'].replace('', np.nan).dropna().apply(
            lambda x: sum(not c.isalpha() for c in x[:-4]) / len(x[:-4]) >= 0.75).sum()

        # print("suspect_name_spelling_cnt", suspect_name_spelling_cnt)
        return suspect_name_spelling_cnt * adjust_name_val

    # try:
    #     suspecious_cwd_cnt = suspect_cwd(df)
    # except Exception as e:
    #     suspecious_cwd_cnt = 0
    #     print(e)

    try:
        suspecious_name_cnt = suspect_name_spelling(df)
    except Exception as e:
        suspecious_name_cnt = 0
        print(e)

    # print([suspecious_cwd_cnt, suspecious_name_cnt])

    return suspecious_name_cnt
