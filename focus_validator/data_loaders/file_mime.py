import magic


def get_file_mime_type(filename):
    f = magic.Magic(uncompress=True)
    return f.from_file(filename=filename)
