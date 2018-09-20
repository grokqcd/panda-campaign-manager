import os, sys, urllib, commands, tempfile

globalTmpDir = ''

# look for a CA certificate directory
def _x509_CApath():
    # use X509_CERT_DIR
    try:
        return os.environ['X509_CERT_DIR']
    except:
        return "/etc/grid-security/certificates"

# curl class
class Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl" '
        # verification of the host certificate
        self.verifyHost = True
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # verbose
        self.verbose = False

    # GET method
    def get(self,url,data,headers=None,rucioAccount=False):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost or not url.startswith('https://'):
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # max time of 10 min
        com += ' -m 600'
        if(headers):
            for h in headers:
                com += ' -H \'%s\'' % h
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()            
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print(com)
            print(strData[:-1])
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
        return ret


    # POST method
    def post(self,url,data,rucioAccount=False):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost or not url.startswith('https://'):
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # max time of 10 min
        com += ' -m 600'
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print("tmpFD",tmpFD,tmpName)
            print(com)
            print(strData[:-1])
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        #os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
        return ret


    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost or not url.startswith('https://'):
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # emulate PUT
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        if self.verbose:
            print(com)
        # execute
        ret = commands.getstatusoutput(com)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
        return ret


    # convert return
    def convRet(self,ret):
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        # add messages to silent errors
        if ret[0] == 35:
            ret = (ret[0],'SSL connect error. The SSL handshaking failed. Check grid certificate/proxy.')
        elif ret[0] == 7:
            ret = (ret[0],'Failed to connect to host.')            
        elif ret[0] == 55:
            ret = (ret[0],'Failed sending network data.')            
        elif ret[0] == 56:
            ret = (ret[0],'Failure in receiving network data.')            
        return ret
    