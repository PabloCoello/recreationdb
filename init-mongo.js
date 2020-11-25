db.createUser(
    {
        user: 'Username',
        pwd: 'password',
        roles: [
            {
                role: 'readWrite',
                db: 'recreationdb'
            }
        ]
    }
)