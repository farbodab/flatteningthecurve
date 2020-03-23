import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-home-graph',
  templateUrl: './home-graph.component.html',
  styleUrls: ['./home-graph.component.scss']
})
export class HomeGraphComponent implements OnInit {

  @Input() graph_data : any;

  constructor() { }

  ngOnInit() {
  }

}
