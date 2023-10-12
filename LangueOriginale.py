
def  LangueOriginaleFct(line):
    reponse=False

    n = line.index(",")

    line2 = line[0:n-1]

    line3 = line[n + 2:]

    m = line3.index(",")
 
    line4 = line3[m+2:m+29]
    if line4=="Langue originale : Français" :
        reponse=True
    return reponse