import os
import re
import argparse
import subprocess
import ftplib

os.environ['HADOOP_CONF_DIR'] = "/opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p0.1580995/lib/spark/conf/yarn-conf"


def checa_arquivo_hadoop(filepath):
    # se o arquivo existir, ele retorna o tamanho do arquivo em bytes,
    # para comparacao
    try:
        output = subprocess.check_output(["hadoop", "fs", "-ls", filepath]).split(" ")[6]
    except:
        output = 0

    return (output)


def procura_maior_arquivo_ftp(detran, usuario, senha):
    # retorna nome do maior arquivo no ftp
    
    files = []
    size = 0
    ftp_bigger_file = ""
    lista_arquivos = []

    fftp = ftplib.FTP(detran)

    fftp.login(usuario, senha)

    fftp.dir(lista_arquivos.append)

    fftp.close()

    for line in lista_arquivos:
        if re.findall('tvep.+', line):
            files.append(line[29:].strip().split(' '))

    for line in files:
        if line[0] > size:
            size = line[0]

    for line in files:
        if line[0] == size:
            nome_maior_arquivo = line[4]
            tamanho_maior_arquivo = line[0]


    return(nome_maior_arquivo, tamanho_maior_arquivo)


def baixar_arquivo_ftp(filename, detran, usuario, senha):
    #baixa arquivo e salva como csv

    csv_filename = re.sub('.txt', '.csv', filename)

    try:
        fftp = ftplib.FTP(detran)
        fftp.login(usuario, senha)
        fftp.retrbinary("RETR " + filename, open(csv_filename, 'wb').write)
        print("Salvando arquivo {} no diretorio local".format(csv_filename))

    except:
        print("Nao foi possivel baixar o arquivo ()".format(filename))

    try:
        fftp.close()
    
    except:
        print("ftp.close nao executado.")

def compara_tamanho_arquivo_ftp_hadoop(tamanho_maior_arquivo_ftp, nome_maior_arquivo_csv):

    try:
        tamanho_arquivo_hadoop = subprocess.check_output(["hadoop", "fs", "-ls", nome_maior_arquivo_csv]).split(" ")[6]
    except:
        tamanho_arquivo_hadoop = 0

    if int(tamanho_maior_arquivo_ftp) > int(tamanho_arquivo_hadoop):
        print("Arquivo no FTP e maior que o existente no HDFS, ou arquivo ainda nao existe")
        return True
    elif int(tamanho_maior_arquivo_ftp) == int(tamanho_arquivo_hadoop):
        print("Arquivos possuem o mesmo tamanho. Nome: {}").format(nome_maior_arquivo_csv)
        return False
    else:
        print("Arquivo no FTP e menor que o existente no HDFS")
        return False


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Dados de acesso ao FTP Detran")
    parser.add_argument('-e','--ftpdetran', metavar='ftpdetran', type=str, help='endereco de acesso ao FTP')
    parser.add_argument('-u','--ftpusuario', metavar='ftpusuario', type=str, help='usuario para acesso ao FTP')
    parser.add_argument('-s','--ftpsenha', metavar='ftpsenha', type=str, help='senha de acesso ao FTP')

    args = parser.parse_args()

    hadoop_path = "/user/mpmapas/staging/detran/detran_veiculo/hivetable/"

    hadoop_path_hist = "/user/mpmapas/staging/detran/detran_veiculo/historico/"

    nome_maior_arquivo, tamanho_maior_arquivo_ftp = procura_maior_arquivo_ftp(args.ftpdetran, args.ftpusuario, args.ftpsenha)

    nome_maior_arquivo_csv = re.sub('.txt', '.csv', nome_maior_arquivo)

    path_maior_arquivo_csv =  hadoop_path + nome_maior_arquivo_csv

    if compara_tamanho_arquivo_ftp_hadoop(tamanho_maior_arquivo_ftp, 
                                            path_maior_arquivo_csv) == True:

        print("baixando arquivo para diretorio local")
        
        baixar_arquivo_ftp(nome_maior_arquivo, args.ftpdetran, args.ftpusuario, args.ftpsenha)

        try:
            print("movendo arquivo existe da pasta HiveTable para Historico")
            subprocess.check_output(["hadoop", "fs", "-mv", hadoop_path + '*.*', hadoop_path_hist])

        except :
            print("Ocorreu um erro movendo arquivo de HiveTable para Histrico")
        
        try:
            print("Movendo arquivo baixado localmente {} para pasta diretorio no HDFS: {}").format(nome_maior_arquivo_csv, hadoop_path)
            subprocess.check_output(["hadoop", "fs", "-moveFromLocal", nome_maior_arquivo_csv, hadoop_path])
        
        except:
            print("Ocorreu um erro ao movimentar o arquivo de local para o HDFS.")
        

    else:
        print('Nao existe arquivo para download no FTP')