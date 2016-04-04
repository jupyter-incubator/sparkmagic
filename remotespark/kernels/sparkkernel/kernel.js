define(['base/js/namespace'], function(IPython){
        var onload = function() {
            IPython.CodeCell.config_defaults.highlight_modes['magic_text/x-sql'] = {'reg':[/^%%sql/]};
            IPython.CodeCell.config_defaults.highlight_modes['magic_text/x-python'] = {'reg':[/^%%local/]};
        }

        return { onload: onload }
    })