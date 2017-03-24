
def bytes_to_kb(bytes):
    return bytes_to_x(bytes, "KB")

def kb_to_bytes(K):
    return x_to_bytes(K, "KB")

def bytes_to_mb(bytes):
    return bytes_to_x(bytes, "MB")


def mb_to_bytes(M):
    return x_to_bytes(M, "MB")


def bytes_to_gb(bytes):
    return bytes_to_x(bytes, "GB")


def gb_to_bytes(G):
    return x_to_bytes(G, "GB")


def bytes_to_tr(bytes):
    return bytes_to_x(bytes, "TB")


def tr_to_bytes(T):
    return x_to_bytes(T, "TB")

lookup = {
    "KB": pow(2,10),
    "MB": pow(2,20),
    "GB": pow(2,30),
    "TB": pow(2,40)
}

def bytes_to_x(bytes, size):
    return bytes / lookup[size]

def x_to_bytes(x, size):
    return x * lookup[size]