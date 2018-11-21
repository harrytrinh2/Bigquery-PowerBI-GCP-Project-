# -*- coding: utf-8 -*-
def insert_upload_order(file_name):
    try:
        masterdata  =  pd.read_excel('C:\\Users\\phuc.trinh\\Documents\\Upload order 2018-11-16\\test\\metadata.xlsx')
        try:
            data = pd.read_excel(file_name)
        except:
            q = "Cannot read ",file_name.split("\\")[-1]
            return q
        data = data.drop(data.index[[0,1,2,3]])
        data = data.rename(columns=data.iloc[0])
        data = data[1:]
        if u'PHASE' not in data.columns:
            q = (file_name.split("\\")[-1]," has a problem with structure")
            return q
        print (data[data[u'ALBUM'].notnull() == True and data[u'HỢP ĐỒNG'].notnull() == True])
        if u'ID VIDEO' in data.columns:
            try:
                try:
                    data = data.drop(u'VIDEO ID', 1)
                except:
                    data = data.drop(u'Video ID', 1)
                try:
                    data.rename(columns={u'ID VIDEO':u'VIDEO ID'},inplace=True)
                except:
                    data.rename(columns={u'ID Video':u'VIDEO ID'},inplace=True)
            except KeyError:
                pass
        if u'ID video' in data.columns:
            try:
                try:
                    data = data.drop(u'VIDEO ID', 1)
                except:
                    data = data.drop(u'Video ID', 1)
                try:
                    data.rename(columns={u'ID video':u'VIDEO ID'},inplace=True)
                except:
                    data.rename(columns={u'ID VIDEO':u'VIDEO ID'},inplace=True)
            except KeyError:
                pass
        if u'id video' in data.columns:
            try:
                try:
                    data = data.drop(u'VIDEO ID', 1)
                except:
                    data = data.drop(u'Video ID', 1)
                try:
                    data.rename(columns={u'id video':u'VIDEO ID'},inplace=True)
                except:
                    data.rename(columns={u'ID VIDEO':u'VIDEO ID'},inplace=True)
            except KeyError:
                pass
        try:
            data[u'ID video Yt']
            data = data.drop(u'Video ID', 1)
            data.rename(columns={u'ID video Yt':u'VIDEO ID'},inplace=True)
        except KeyError:
            pass
        try:
            data[u'Video ID']
            data.rename(columns={u'Video ID':u'VIDEO ID'},inplace=True)
        except KeyError:
            pass
        if (str(data.columns[1])=='nan' and str(data.columns[2])=='nan'):
            try:
                data = data.drop('VIDEO ID',1)
            except KeyError:
                data = data.drop('Video ID',1)
            data = data[data.iloc[:, 1].notnull()==True]
            column_names = data.columns.values
            column_names[1] = u'VIDEO ID'
            data.columns = column_names
        data = data[data['VIDEO ID'].notnull()]
        if 'ALBUM' in data.columns  and len(data) >200:
            copyfile(file_name,"C:\\Users\\phuc.trinh\\Documents\\Upload order 2018-11-16\\test\\undone\\"+file_name.split("\\")[-1])
            os.remove(file_name)
            q = (file_name.split("\\")[-1],"contains Album")
            return q
        else:
            try:
                data['TÊN NỘI DUNG']
            except KeyError:
                try:
                    data[u'NỘI DUNG']
                    data = data.drop(u'TÊN NỘI DUNG', 1)
                    data.rename(columns={u'NỘI DUNG':u'TÊN NỘI DUNG'},inplace=True)
                except KeyError:
                    try:
                        data[u'TÊN BÀI HÁT']
                        data.rename(columns={u'TÊN BÀI HÁT':u'TÊN NỘI DUNG'},inplace=True)
                    except KeyError:
                        pass
                    try:
                        data[u'Tên bài hát | Tên ca sĩ /Lyric Video']
                        data.rename(columns={'Tên bài hát | Tên ca sĩ /Lyric Video':u'TÊN NỘI DUNG'},inplace=True)
                    except KeyError:
                        pass
                    try:
                        data[u'TÊN PHIM']
                        data.rename(columns={u'TÊN PHIM':u'TÊN NỘI DUNG'},inplace=True)
                    except KeyError:
                        pass
        try:
            data[u'Series Name']
            data.rename(columns={u'Series Name':u'SERIES NAME'},inplace=True)
        except KeyError:
            pass
        try:
            data[u'Series Name']
            data.rename(columns={u'Series Name':u'Series Name'},inplace=True)
        except KeyError:
            pass
        try:
            merge_data =  data[[u'VIDEO ID',u'TÊN NỘI DUNG',u'HỢP ĐỒNG',u'CATEGORY',u'SUB CATEGORY',u'SUB CATEGORY 2',u'SEASON',u'PHASE',u'SERIES NAME']]
            frames = [masterdata,merge_data]
            result = pd.concat(frames)
            result.to_excel('C:\\Users\\phuc.trinh\\Documents\\Upload order 2018-11-16\\test\\metadata.xlsx')
            q = (file_name.split("\\")[-1] , "is succeed")
        except:
            copyfile(file_name,"C:\\Users\\phuc.trinh\\Documents\\Upload order 2018-11-16\\test\\undone\\"+file_name.split("\\")[-1])
            os.remove(file_name)
            q = (file_name.split("\\")[-1]," has a problem with merging data")
        pass
    except (IndexError) as e:
        copyfile(file_name,"C:\\Users\\phuc.trinh\\Documents\\Upload order 2018-11-16\\test\\undone\\"+file_name.split("\\")[-1])
        os.remove(file_name)
        q = (file_name.split("\\")[-1]," has a problem with structure")
        pass
    return q