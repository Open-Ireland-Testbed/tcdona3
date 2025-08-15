def Send(Connexion, Command):
    from .Constantes import APXXXX_ERROR_ARGUMENT_TYPE, APXXXX_ERROR_BADCOMMAND
    from .Errors import ApexError
    from sys import exit
    from socket import timeout

    if not isinstance(Command, str):
        Connexion.close()
        raise ApexError(APXXXX_ERROR_ARGUMENT_TYPE, "Command")
    try:
        Connexion.send(Command.encode("utf-8"))
    except timeout:
        Connexion.close()
        raise ApexError(APXXXX_ERROR_BADCOMMAND, Command)


def Receive(Connexion, ByteNumber=1024):

    from sys import exit
    from socket import timeout
    from .Errors import ApexError
    from .Constantes import APXXXX_ERROR_ARGUMENT_TYPE, APXXXX_ERROR_COMMUNICATION

    if not isinstance(ByteNumber, int):
        Connexion.close()
        raise ApexError(APXXXX_ERROR_ARGUMENT_TYPE, "ByteNumber")
    try:
        data = Connexion.recv(ByteNumber)
    except timeout:
        Connexion.close()
        raise ApexError(APXXXX_ERROR_COMMUNICATION, Connexion.getsockname()[0])
    else:
        return data.decode("utf-8")


# Python Socket Receive Large Amount of Data
#  The line data += packet can make receiving VERY slow for large messages.
#  It's much better to use data = bytearray() and then data.extend(packet).
def recvall(Connexion, ByteNumber):
    # # Helper function to recv n bytes or return None if EOF is hit
    # data = bytearray()
    # while len(data) < n:
    #     packet = Connexion.recv(n - len(data))
    #     if not packet:
    #         return None
    #     data.extend(packet)
    # return data

    from .Constantes import (
        APXXXX_ERROR_ARGUMENT_TYPE,
        APXXXX_ERROR_COMMUNICATION,
    )
    from .Errors import ApexError

    # from sys import exit
    from socket import timeout

    if not isinstance(ByteNumber, int):
        Connexion.close()
        raise ApexError(APXXXX_ERROR_ARGUMENT_TYPE, "ByteNumber")
    try:
        # Helper function to recv n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < ByteNumber:
            packet = Connexion.recv(ByteNumber - len(data))
            if not packet:
                return None
            data.extend(packet)
    except timeout:
        Connexion.close()
        raise ApexError(APXXXX_ERROR_COMMUNICATION, Connexion.getsockname()[0])
    else:
        return data


def ReceiveUntilChar(Connexion, EndCharacter="\n"):
    from .Constantes import (
        APXXXX_ERROR_ARGUMENT_TYPE,
        APXXXX_ERROR_COMMUNICATION,
    )
    from .Errors import ApexError
    from sys import exit
    from socket import timeout

    if not isinstance(EndCharacter, str):
        Connexion.close()
        raise ApexError(APXXXX_ERROR_ARGUMENT_TYPE, "EndCharacter")
    try:
        data_total = ""
        while True:
            data = (Connexion.recv(1024)).decode("utf-8")
            if data.find(EndCharacter) >= 0:
                data_total += data[: data.find(EndCharacter)] + EndCharacter
                break
            else:
                data_total += data
    except timeout:
        Connexion.close()
        raise ApexError(APXXXX_ERROR_COMMUNICATION, Connexion.getsockname()[0])
    else:
        return data_total
