def get_sftp():
    print('sftp 작업을 시작합니다.')
def regist2(name, sex, *args, **kwars):
    print(name)
    print(sex)
    print(*args)
    email = kwargs['email'] or None
    phone = kwargs['phone'] or None

    if email: 
        print(f'email 주소: {email}')
    if phone: 
        print(f'phone 번호: {phone}')

