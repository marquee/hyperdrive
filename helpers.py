import settings

def _loadPublication():
    """
    Private: load the Publication specified in settings from the API.

    Returns the active Publication.
    """

    pub_key = "publication:{}".format(settings.PUBLICATION_SHORT_NAME)

    publication_container = json.loads(r.hgetall(pub_key)['object'])
    publication = Publication(publication_container)
    return None