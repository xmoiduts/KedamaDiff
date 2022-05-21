#.....................................
#.....WARNING, EXPERIMENTAL USAGE.....
#.....................................

#def path2xy(path, depth):
#    """由图块路径转项目定义的坐标。
#    
#    传参时应保证path与给定爬图等级相符，即 path = total_depth + target_depth 
#    
#    Args: E.g. : '/0/3/3/3/1/2/1/3' , 15
#        path (int) : The overviewer img block path to convert.
#            P.S.: The '/' in the beginning is needed
#        depth (int) : The total zoom-levels for the given overviewer 
#            map (total depth)."""
#
#    in_list = map(int, path.split('/')[1:])
#    X, Y = (0, 0)
#    table = [1, 3, 0, 2]
#    for index, value in enumerate(in_list):
#        X += (table[value]//2-0.5)*2**(depth-index)  # 需要整数除
#        Y += (table[value] % 2-0.5)*2**(depth-index)
#    return(int(X), int(Y))

# New function.
def ancestorOf(path:str, levels:int):
    """获取指定图块路径的第[levels]级祖先图块路径。
    例如： /0/1/2/3/3/2 的第0级祖先图块路径是 /0/1/2/3/3/2，
    /0/1/2/3/3/2 的第3级祖先图块路径是 /0/1/2
    Args:
        path: path of the image tile (per KedamaDiff coordinate system)
        levels: a non-positive integer, specifies how many levels
            the ancestor should be above the [path] image.
            ancestor's level shall not be above or at root (which depth is 0)
    Returns:
        ancestor_path (str)
    """
    assert path.startswith('/')
    child_path_list = path.split('/')[1:]
    assert 0 <= -levels < len(child_path_list)
    ancestor_path = '/' + '/'.join(child_path_list[:len(child_path_list)+levels])
    return ancestor_path
