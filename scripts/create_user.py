#!/usr/bin/env python3
import boto3
import sys

cognito = boto3.client('cognito-idp')

USER_POOL_ID = 'COLE_AQUI_O_USER_POOL_ID'  # Pegar do output do CDK

def create_user(username, email, password):
    try:
        # Criar usuário
        response = cognito.admin_create_user(
            UserPoolId=USER_POOL_ID,
            Username=username,
            UserAttributes=[
                {'Name': 'email', 'Value': email},
                {'Name': 'email_verified', 'Value': 'true'}
            ],
            TemporaryPassword=password,
            MessageAction='SUPPRESS'
        )
        
        # Definir senha permanente
        cognito.admin_set_user_password(
            UserPoolId=USER_POOL_ID,
            Username=username,
            Password=password,
            Permanent=True
        )
        
        print(f"✓ Usuário criado: {username}")
        print(f"  Email: {email}")
        print(f"  Senha: {password}")
        
    except Exception as e:
        print(f"✗ Erro: {str(e)}")

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Uso: python create_user.py <username> <email> <password>")
        sys.exit(1)
    
    create_user(sys.argv[1], sys.argv[2], sys.argv[3])
