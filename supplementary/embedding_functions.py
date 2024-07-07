def generate_random_walks(vertices, edges, num_walks=5, len_walks=20):
    """
    PySpark function to simulate num_walks random walks per vertex in a graph, with maximum length len_walks.
    In total num_walks * vertices.count() random walks will be created.
    Parameters
    ----------
    vertices : vertices RDD of graph
    edges : edges RDD of graph
    num_walks : number
    len_walks :

    Returns
    -------
    An RDD of random walks
    """
    import random
    walkers = vertices.flatMap(lambda id: [(id[0], (num, [id[0]])) for num in range(num_walks)])
    for _ in range(len_walks - 1):
        walkers = walkers.leftOuterJoin(edges) \
            .map(lambda row: ((row[0], row[1][0][0]), (row[1][0][1], [row[1][1]]))) \
            .reduceByKey(lambda a, b: (a[0], a[1] + b[1])) \
            .map(lambda row: (row[0], row[1][0] + [random.choice(row[1][1])])) \
            .map(lambda row: (row[0], [item for item in row[1] if item is not None])) \
            .map(lambda row: (row[1][-1], (row[0][1], row[1]))) \
            .coalesce(200)
    return walkers.map(lambda x: x[1][1])


def embed_walks(walks, vector_size=20, dimension=2, seed=42):
    """
    Feeds a set of random walks in a graph into the Word2Vec algorithm in the PySpark MMLib.
    This produces multi-dimensional vector representations of the vertices, each with vector_size dimensions.
    The t-SNE algorithm is then used to embed the vertex vectors onto a new dimensional space, default 2-dim.
    Parameters
    ----------
    walks : RDD of random walks in the graph
    vector_size : length of vectors to be output by the Word2Vec algorithm
    dimension : size of the t-SNE embeddings of the vectors
    seed : set the seed for the Word2Vec algorithm

    Returns
    -------
    vectors: list of (user, Word2Vec vector) pairs
    users: list of users
    embeddings: the t-SNE embedding of the Word2Vec vectors
    model: Word2Vec model
    tsne_2d: t-SNE model
    """
    from pyspark.mllib.feature import Word2Vec
    from sklearn.manifold import TSNE

    model = Word2Vec().setVectorSize(20).setSeed(seed).fit(walks)
    vectors = model.getVectors()
    embeddings = [vector for _, vector in vectors.items()]
    users = vectors.keys()
    tsne_2d = TSNE(n_components=2, verbose=1, random_state=0, init='pca')
    embeddings_2d = tsne_2d.fit_transform(embeddings)

    return vectors, users, embeddings_2d, model, tsne_2d


def plot_grouped_embedding(data, group_by, title, size=7, aspect=1.5, alpha=0.5):
    """
    Plot the 2-dimensional data and colour the scatter plot values by a categorical characteristic.
    Parameters
    ----------
    data : 2-dimensional data to plot
    group_by : discrete factor to colour the scatter points by
    title : title of graph
    size : size of graph
    aspect : aspect of the graph
    alpha : opaqueness of the scatter points

    Returns
    -------
    Plot of the graph data on a 2-dimensional graph.
    """

    import seaborn as sns
    import matplotlib.pyplot as plt

    data_to_plot = data.groupby(group_by)
    _hue_order = data[group_by].unique()
    sns.set(style='ticks')
    fg = sns.FacetGrid(data=data_to_plot, hue=group_by, hue_order=_hue_order, aspect=aspect, size=size)
    fg.map(plt.scatter, 'x', 'y', alpha=alpha).add_legend()
    plt.title(title)


def plot_gradient_embedding(data, colorby, title, figsize=(12, 10), alpha=0.7):
    """
    Plot the 2-dimensional data and colour the scatter plot values by a continuous characteristic.
    Parameters
    ----------
    data : 2-dimensional data to plot
    colorby : continuous factor to colour the scatter points by
    title : title of graph
    figsize : size of the plotted figure
    alpha : opaqueness of the scatter points

    Returns
    -------
    Plot of the graph data on a 2-dimensional graph.
    """
    import seaborn as sns
    import matplotlib.pyplot as plt
    cmap = sns.cubehelix_palette(as_cmap=True)
    fig, ax = plt.subplots(figsize=figsize)
    points = ax.scatter(data_to_plot["x"], data_to_plot["y"], c=data_to_plot[colorby], s=50, cmap=cmap, alpha=alpha)
    fig.colorbar(points)
    plt.title(title)
