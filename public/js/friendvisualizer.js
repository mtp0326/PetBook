
$(document).ready(function() {
  $.getJSON('/friendvisualization', function (json) {
      console.log(json)
      var infovis = document.getElementById('infovis');
      var w = infovis.offsetWidth - 50, h = infovis.offsetHeight - 50;

      //init Hypertree
      var ht = new $jit.Hypertree({
        //id of the visualization container
        injectInto: 'infovis',
        //canvas width and height
        width: w,
        height: h,
        //Change node and edge styles such as
        //color, width and dimensions.
        Node: {
            //overridable: true,
            'transform': false,
            color: "#f00"
        },
    
        Edge: {
            //overridable: true,
            color: "#088"
        },
        //calculate nodes offset
        offset: 0.2,
        //Change the animation transition type
        transition: $jit.Trans.Back.easeOut,
        //animation duration (in milliseconds)
        duration:1000,
        //Attach event handlers and add text to the
        //labels. This method is only triggered on label
        //creation

        onCreateLabel: function(domElement, node){
            domElement.innerHTML = node.name;
            domElement.style.cursor = "pointer";
            domElement.onclick = function() {
                $.getJSON('/getFriends/'+node.id, function(json) {
                    ht.op.sum(json, {
                        type: "fade:seq",
                        fps: 30,
                        duration: 1000,
                        hideLabels: false,
                        onComplete: function(){
                            console.log("New nodes added!");
                        }
                    });
                });
            }
        },
        //Change node styles when labels are placed
        //or moved.
        onPlaceLabel: function(domElement, node){
                var width = domElement.offsetWidth;
                var intX = parseInt(domElement.style.left);
                intX -= width / 2;
                domElement.style.left = intX + 'px';
        },
        
        onComplete: function(){
        }
      });
      //load JSON data.
      ht.loadJSON(json);
      //compute positions and plot.
      ht.refresh();
      //end
      ht.controller.onBeforeCompute(ht.graph.getNode(ht.root));
      ht.controller.onAfterCompute();
      ht.controller.onComplete();
    });
});
